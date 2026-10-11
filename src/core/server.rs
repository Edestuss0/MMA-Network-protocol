use crate::core::commands::Response;
use crate::core::config::Config;
use crate::core::router::Router;
use crate::protocol::framer::Framer;
use futures_util::{SinkExt, StreamExt};
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio::net::TcpStream;
use crate::core::config::FramerConfig;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::codec::Framed;

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

pub struct Server {
    config: Config,
    tcp_listener: TcpListener,
    stop: CancellationToken,
}

impl Server {
    pub async fn new(config: Config) -> Result<Self, std::io::Error> {
        Self::validate_config(&config)?;
        let listener = TcpListener::bind(config.address).await?;
        Self::from_listener(config, listener)
    }

    pub fn from_listener(
        config: Config,
        tcp_listener: TcpListener,
    ) -> Result<Self, std::io::Error> {
        Self::validate_config(&config)?;
        Ok(Self {
            config,
            tcp_listener,
            stop: CancellationToken::new(),
        })
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.stop.clone()
    }

    fn validate_config(config: &Config) -> Result<(), io::Error> {
        if config.frame_config.validate().is_err() || config.max_in_flight == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid server configuration",
            ));
        }
        Ok(())
    }
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, std::io::Error> {
        self.tcp_listener.local_addr()
    }

    pub async fn run(&mut self, router: Router) -> Result<(), std::io::Error> {
        println!("Server listen on {} now", self.config.address);
        self.run_until(router, async { let _ = tokio::signal::ctrl_c().await; }).await
    }

    pub async fn run_until<F: Future<Output = ()>>(
        &mut self,
        router: Router,
        shutdown: F,
    ) -> Result<(), std::io::Error> {
        let router = Arc::new(router);
        let semaphore = Arc::new(Semaphore::new(self.config.max_in_flight as usize));
        let stop = self.stop.clone();
        let kill = CancellationToken::new();
        let conns = TaskTracker::new();
        let mut result = Ok(());
        tokio::pin!(shutdown);

        loop {
            let socket = tokio::select! {
                _ = &mut shutdown => break,
                _ = stop.cancelled() => break,
                acc = self.tcp_listener.accept() => match acc {
                    Ok((socket, _)) => socket,
                    Err(e) => { result = Err(e); break; }
                },
            };
            let _ = socket.set_nodelay(true);
            conns.spawn(serve_connection(
                socket,
                Arc::clone(&router),
                self.config.frame_config.clone(),
                self.config.max_batch,
                Arc::clone(&semaphore),
                stop.clone(),
                kill.clone(),
            ));
        }

        stop.cancel();
        conns.close();
        if tokio::time::timeout(SHUTDOWN_GRACE, conns.wait()).await.is_err() {
            eprintln!("graceful shutdown: grace period expired, killing connections");
            kill.cancel();
            conns.wait().await;
        }
        result
    }
}

async fn serve_connection(
    socket: TcpStream,
    router: Arc<Router>,
    frame_config: FramerConfig,
    max_batch: u8,
    semaphore: Arc<Semaphore>,
    stop: CancellationToken,
    kill: CancellationToken,
) {
    let framed = Framed::new(socket, Framer::new(frame_config));
    let (mut sink, mut stream) = framed.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Response>(1000);
    let inflight = TaskTracker::new();

    let writer = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if let Err(e) = sink.feed(frame).await {
                eprintln!("Error sending frame: {}", e);
                return;
            }
            let mut count = 1;
            while count < max_batch {
                match rx.try_recv() {
                    Ok(x) => {
                        if sink.feed(x).await.is_err() {
                            return;
                        }
                        count += 1;
                    }
                    Err(_) => break,
                }
            }
            if let Err(e) = sink.flush().await {
                eprintln!("Error flushing sink: {}", e);
                return;
            }
        }
        let _ = sink.close().await;
    });

    loop {
        let frame = tokio::select! {
            biased;
            _ = kill.cancelled() => break,
            _ = stop.cancelled() => break,
            f = stream.next() => match f { Some(f) => f, None => break },
        };
        match frame {
            Ok(cmd) => {
                let permit = tokio::select! {
                    _ = stop.cancelled() => break,
                    p = Arc::clone(&semaphore).acquire_owned() => p.unwrap(),
                };
                let tx = tx.clone();
                let router = Arc::clone(&router);
                inflight.spawn(async move {
                    let _permit = permit;
                    let response = router.handle(cmd);
                    if let Err(e) = tx.send(response).await {
                        eprintln!("{}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("{:?}", e);
                break;
            }
        }
    }

    inflight.close();
    tokio::select! { _ = inflight.wait() => {}, _ = kill.cancelled() => {} }
    drop(tx);
    if kill.is_cancelled() {
        writer.abort();
    } else {
        let _ = writer.await;
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, async {
            while let Some(Ok(_)) = stream.next().await {}
        }).await;
    }
}