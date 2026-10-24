use crate::core::commands::ChannelSendedMessage;
use crate::core::commands::Response;
use crate::core::config::Config;
use crate::core::router::Router;
use crate::network::network::SocketStatus;
use crate::network::network::serve_connection;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::{io, panic};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug)]
pub struct Server {
    config: Config,
    tcp_listener: TcpListener,
    stop: CancellationToken,
    channel_sender: broadcast::Sender<ChannelSendedMessage>,
}

#[derive(Clone)]
pub struct ServerHandle {
    channel_sender: broadcast::Sender<ChannelSendedMessage>,
}

impl ServerHandle {
    pub fn send_to_channel(&self, conn_id: u32, message: Response) -> Result<(), ()> {
        let sender = self.channel_sender.clone();
        if let Err(_) = sender.send(ChannelSendedMessage { message, conn_id }) {
            return Err(());
        }
        Ok(())
    }
}

impl Server {
    pub async fn new(config: Config) -> Result<Self, std::io::Error> {
        Self::validate_config(&config)?;
        let listener = TcpListener::bind(config.address).await?;
        Self::from_listener(config, listener)
    }

    pub fn handle(&self) -> ServerHandle {
        ServerHandle {
            channel_sender: self.channel_sender.clone(),
        }
    }

    pub fn from_listener(
        config: Config,
        tcp_listener: TcpListener,
    ) -> Result<Self, std::io::Error> {
        Self::validate_config(&config)?;
        let (channel_sender, _) = broadcast::channel::<ChannelSendedMessage>(1024);
        Result::Ok(Self {
            config,
            tcp_listener,
            stop: CancellationToken::new(),
            channel_sender,
        })
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.stop.clone()
    }

    pub fn shutdown_server(&self) {
        self.stop.cancel();
    }

    fn validate_config(config: &Config) -> Result<(), io::Error> {
        if let Err(e) = config.validate() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, e));
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

        let current_channel_connection_id: Arc<AtomicU32> = Arc::new(AtomicU32::new(1));

        loop {
            let conns_semaphore = Arc::clone(&conns_semaphore);
            let current_channel_connection_id = Arc::clone(&current_channel_connection_id);
            let mut socket = tokio::select! {
                _ = &mut shutdown => break,
                _ = stop.cancelled() => break,
                acc = self.tcp_listener.accept() => match acc {
                    Result::Ok((socket, _)) => socket,
                    Result::Err(e) => { result = Err(e); continue; }
                },
            };
            let permit = match conns_semaphore.try_acquire_owned() {
                Result::Ok(p) => p,
                Err(_) => {
                    socket.shutdown().await?;
                    continue;
                }
            };

            let _ = socket.set_nodelay(true);
            let channel_receiver = self.channel_sender.subscribe();

            conns.spawn(serve_connection(
                socket,
                Arc::clone(&router),
                self.config.clone(),
                Arc::clone(&semaphore),
                stop.clone(),
                kill.clone(),
                permit,
                channel_receiver,
                move || {
                    let conn_id = current_channel_connection_id.fetch_add(1, Ordering::Relaxed);
                    println!("connect, {}", conn_id);
                    SocketStatus::Channel(conn_id)
                },
            ));
        }

        stop.cancel();
        conns.close();
        if tokio::time::timeout(self.config.shutdown_grace, conns.wait())
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
