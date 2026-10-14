use crate::core::commands::RequestOpcode;
use crate::core::commands::Response;
use crate::core::commands::ResponseOpcode::Ok;
use crate::core::config::Config;
use crate::core::config::FramerConfig;
use crate::core::router::Router;
use crate::protocol::framer::Framer;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::future::Future;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::{io, panic};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
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
        Result::Ok(Self {
            config,
            tcp_listener,
            stop: CancellationToken::new(),
        })
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.stop.clone()
    }

    pub fn shutdown_server(&self) {
        self.stop.cancel();
    }

    fn validate_config(config: &Config) -> Result<(), io::Error> {
        if config.frame_config.validate().is_err() || config.max_in_flight == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid server configuration",
            ));
        }
        Result::Ok(())
    }
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, std::io::Error> {
        self.tcp_listener.local_addr()
    }

    pub async fn run(&mut self, router: Router) -> Result<(), std::io::Error> {
        println!("Server listen on {} now", self.config.address);
        self.run_until(router, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
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
        let mut result = Result::Ok(());
        tokio::pin!(shutdown);

        let conns_semaphore = Arc::new(Semaphore::new(self.config.max_connections as usize));

        let current_channel_connection_id: Arc<RwLock<u32>> = Arc::new(RwLock::new(0));

        loop {
            let conns_semaphore = Arc::clone(&conns_semaphore);
            let current_channel_connection_id = Arc::clone(&current_channel_connection_id);
            let mut socket = tokio::select! {
                _ = &mut shutdown => break,
                _ = stop.cancelled() => break,
                acc = self.tcp_listener.accept() => match acc {
                    Result::Ok((socket, _)) => socket,
                    Result::Err(e) => { result = Err(e); break; }
                },
            };
            let permit = match conns_semaphore.try_acquire_owned() {
                Result::Ok(p) => p,
                Err(_) => {
                    socket.shutdown().await;
                    continue;
                }
            };

            let _ = socket.set_nodelay(true);

            let conn_id: Arc<RwLock<Option<u32>>> = Arc::new(RwLock::new(None));
            let conn_id_for_closure = Arc::clone(&conn_id); 
            let is_channel = Arc::new(RwLock::new(false));
            let is_channel_for_closure = Arc::clone(&is_channel);
            conns.spawn(serve_connection(
                socket,
                Arc::clone(&router),
                self.config.frame_config.clone(),
                self.config.max_batch,
                Arc::clone(&semaphore),
                stop.clone(),
                kill.clone(),
                permit,
                conn_id,
                is_channel,
                move || {
                    let current_id = match current_channel_connection_id.read() {
                        Result::Ok(i) => *i,
                        Err(_) => {
                            return Err(());
                        }
                    };
                    let current_channel_guard = current_channel_connection_id.write();
                    let conn_id_guard = conn_id_for_closure.write();
                    let is_channel_guard = is_channel_for_closure.write();

                    if !current_channel_guard.is_err() && !conn_id_guard.is_err() && !is_channel_guard.is_err() {
                        *current_channel_guard.unwrap() += 1;
                        *conn_id_guard.unwrap() = Some(current_id + 1);
                        *is_channel_guard.unwrap() = true;
                    } else {
                        return Err(());
                    }

                    let id = match conn_id_for_closure.read() {
                        Result::Ok(i) => match *i {
                            Some(p) => p,
                            None => {
                                return Err(());
                            }
                        },
                        Err(_) => {
                            return Err(())
                        }
                    };
                    let connected = match is_channel_for_closure.read() {
                        Result::Ok(i) => *i,
                        Err(_) => {
                            return Err(());
                        }
                    };

                    Result::Ok((id, connected))
                },
            ));
        }

        stop.cancel();
        conns.close();
        if tokio::time::timeout(SHUTDOWN_GRACE, conns.wait())
            .await
            .is_err()
        {
            eprintln!("graceful shutdown: grace period expired, killing connections");
            kill.cancel();
            conns.wait().await;
        }
        result
    }
}

async fn serve_connection<F>(
    socket: TcpStream,
    router: Arc<Router>,
    frame_config: FramerConfig,
    max_batch: u8,
    semaphore: Arc<Semaphore>,
    stop: CancellationToken,
    kill: CancellationToken,
    conn_permit: OwnedSemaphorePermit,
    conn_id: Arc<RwLock<Option<u32>>>,
    is_channel: Arc<RwLock<bool>>,
    on_channel_connect: F,
) where
    F: Fn() -> Result<(u32, bool), ()> + Send + Sync + 'static,
{
    let framed = Framed::new(socket, Framer::new(frame_config));
    let (mut sink, mut stream) = framed.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Response>(1000);
    let inflight = TaskTracker::new();

    let conn_semaphore = Arc::new(Semaphore::new(256));

    let writer = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if let Err(e) = sink.feed(frame).await {
                eprintln!("Error sending frame: {}", e);
                return;
            }
            let mut count = 1;
            while count < max_batch {
                match rx.try_recv() {
                    Result::Ok(x) => {
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

    let on_channel_connect = Arc::new(on_channel_connect);

    loop {
        let frame = tokio::select! {
            biased;
            _ = kill.cancelled() => break,
            _ = stop.cancelled() => break,
            f = stream.next() => match f { Some(f) => f, None => break },
        };

        let is_channel = Arc::clone(&is_channel);

        match frame {
            Result::Ok(cmd) => {
                let (permit_global, permit_local) = tokio::select! {
                    _ = stop.cancelled() => break,
                    per = Arc::clone(&conn_semaphore).acquire_owned() => {
                        let permit_local = Arc::clone(&semaphore).acquire_owned().await;
                        (per.unwrap(), permit_local.unwrap())
                    }
                };

                let tx = tx.clone();
                let router = Arc::clone(&router);
                let conn_id = Arc::clone(&conn_id);
                let req_id = cmd.headers.req_id;

                let on_channel_connect = Arc::clone(&on_channel_connect);                

                inflight.spawn(async move {
                    let permit = (permit_global, permit_local);

                    let mut is_channel_val = {
                        let guard = is_channel.read().unwrap();
                        *guard
                    };

                    let mut conn_id_val = {
                        let guard = conn_id.read().unwrap();
                        *guard
                    };

                    if cmd.headers.opcode != RequestOpcode::ChannelConnect
                        && cmd.headers.opcode != RequestOpcode::ChannelMessage
                        && is_channel_val
                    {
                        if let Err(e) = tx.send(Response::connection_type_mismatch(req_id)).await {
                            eprintln!("{}", e);
                        }
                        return;
                    }

                    if cmd.headers.opcode == RequestOpcode::ChannelConnect
                        && !is_channel_val
                        && conn_id_val == None
                    {
                        match on_channel_connect() {
                            Result::Ok((id, connected)) => {
                                is_channel_val = connected;
                                conn_id_val = Some(id);
                                if let Err(e) = tx.send(Response::channel_connected(req_id)).await {
                                    eprintln!("{}", e);
                                }
                                println!("{}, {:?}", is_channel_val, conn_id_val);
                            }
                            Err(_) => {
                                if let Err(e) = tx.send(Response::bad_request(req_id, Some(Bytes::from("Failed to connect channel")))).await {
                                    eprintln!("{}", e);
                                }
                            }
                        }
                        return;
                    }

                    let response =
                        match panic::catch_unwind(panic::AssertUnwindSafe(|| router.handle(cmd))) {
                            Result::Ok(r) => r,
                            Err(_) => Response::internal_error(req_id),
                        };

                    drop(permit);

                    if let Err(e) = tx.send(response).await {
                        eprintln!("{}", e);
                    }
                });
            }
            Err(error) => {
                if let Err(e) = tx
                    .send(Response::bad_request(
                        0,
                        Some(Bytes::from(error.to_string())),
                    ))
                    .await
                {
                    eprintln!("Error with sending to socket: {:?}", e);
                }
                break;
            }
        }
    }

    inflight.close();
    tokio::select! { _ = inflight.wait() => {}, _ = kill.cancelled() => {} }
    drop(tx);
    drop(conn_permit);
    if kill.is_cancelled() {
        writer.abort();
    } else {
        let _ = writer.await;
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, async {
            while let Some(Result::Ok(_)) = stream.next().await {}
        })
        .await;
    }
}
