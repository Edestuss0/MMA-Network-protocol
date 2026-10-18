use crate::core::commands::ChannelSendedMessage;
use crate::core::commands::Request;
use crate::core::commands::RequestOpcode;
use crate::core::commands::Response;
use crate::core::config::Config;
use crate::core::config::FramerConfig;
use crate::core::router::Router;
use crate::protocol::framer::Framer;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast;
use tokio::sync::mpsc::Receiver;
use std::future::Future;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
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

#[derive(Debug,)]
pub struct Server {
    config: Config,
    tcp_listener: TcpListener,
    stop: CancellationToken,
    channel_sender: broadcast::Sender<ChannelSendedMessage>,
}

pub struct ServerHandle {
    channel_sender: broadcast::Sender<ChannelSendedMessage>,
}

#[derive(Debug, Clone, Copy)]
enum SocketStatus { Fresh, Once, Channel(u32) }

impl SocketStatus {
    fn to_u64(self) -> u64 {
        match self {
            SocketStatus::Fresh => 0,
            SocketStatus::Once => 1,
            SocketStatus::Channel(id) => (2 << 32) | (id as u64),
        }
    }

    fn from_u64(val: u64) -> Self {
        let tag = val >> 32;
        match tag {
            0 => SocketStatus::Fresh,
            1 => SocketStatus::Once,
            2 => SocketStatus::Channel(val as u32),
            _ => unreachable!(),
        }
    }
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                e,
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
                    socket.shutdown().await;
                    continue;
                }
            };

            let _ = socket.set_nodelay(true);

            // let conn_id: Arc<RwLock<Option<u32>>> = Arc::new(RwLock::new(None));
            let channel_receiver = self.channel_sender.subscribe();

            conns.spawn(serve_connection(
                socket,
                Arc::clone(&router),
                self.config.frame_config.clone(),
                self.config.max_batch,
                Arc::clone(&semaphore),
                stop.clone(),
                kill.clone(),
                permit,
                channel_receiver,
                move || {
                    let conn_id =  current_channel_connection_id.fetch_add(1, Ordering::Relaxed);
                    println!("connect, {}", conn_id);
                    SocketStatus::Channel(conn_id)
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
    max_batch: u32,
    semaphore: Arc<Semaphore>,
    stop: CancellationToken,
    kill: CancellationToken,
    conn_permit: OwnedSemaphorePermit,
    mut channel_receiver: broadcast::Receiver<ChannelSendedMessage>,
    on_channel_connect: F,
) where
    F: Fn() -> SocketStatus + Send + Sync + 'static,
{
    let framed = Framed::new(socket, Framer::new(frame_config));
    let (mut sink, mut stream) = framed.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Response>(1000);
    let inflight = TaskTracker::new();

    let conn_semaphore = Arc::new(Semaphore::new(256));
    let tx_copy = tx.clone();

    let on_channel_connect = Arc::new(on_channel_connect);
    let socket_status = Arc::new(AtomicU64::new(SocketStatus::to_u64(SocketStatus::Fresh)));
    let channel_socket_status = Arc::clone(&socket_status);

    let channel_sender = tokio::spawn(async move {
        while let Ok(message) = channel_receiver.recv().await {
            let conn_id = match SocketStatus::from_u64(channel_socket_status.load(Ordering::Relaxed)) {
                SocketStatus::Channel(x) => x,
                _ => continue
            };

            if message.conn_id == conn_id || message.conn_id == 0 {
                if let Err(e) = tx_copy.send(message.message).await {
                    eprintln!("{}", e);
                }
            }
        }
    });

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

    loop {
        let frame = tokio::select! {
            biased;
            _ = kill.cancelled() => break,
            _ = stop.cancelled() => break,
            f = stream.next() => match f { Some(f) => f, None => break },
        };

        
        match frame {
            Result::Ok(cmd) => {
                let (permit_local, permit_global) = tokio::select! {
                    _ = stop.cancelled() => break,
                    per = Arc::clone(&conn_semaphore).acquire_owned() => {
                        let permit_local = Arc::clone(&semaphore).acquire_owned().await;
                        (per.unwrap(), permit_local.unwrap())
                    }
                };
                
                let tx = tx.clone();
                let router = Arc::clone(&router);
                let on_channel_connect = Arc::clone(&on_channel_connect);
                let socket_status = Arc::clone(&socket_status);

                inflight.spawn(async move {
                    if let Some(r) = handle_cmd(cmd, socket_status, permit_global, router, on_channel_connect).await {
                        if let Err(e) = tx.send(r).await {
                            eprintln!("{}", e)
                        }
                    }
                    drop(permit_local);

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
    channel_sender.abort();
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

async fn handle_cmd<F>(cmd: Request, socket_status: Arc<AtomicU64>, permit: OwnedSemaphorePermit, router: Arc<Router>, on_channel_connect: Arc<F>
) -> Option<Response> where F: Fn() -> SocketStatus + Send + Sync + 'static {
    let req_id = cmd.headers.req_id;

    match SocketStatus::from_u64(socket_status.load(Ordering::Relaxed)) {
        SocketStatus::Fresh => {
            match cmd.headers.opcode {
                RequestOpcode::Once => {
                    socket_status.store(SocketStatus::to_u64(SocketStatus::Once), Ordering::Relaxed);

                    // if lock_result.is_err() {
                    //     drop(permit);

                    //     return Some(Response::internal_error(req_id));
                    // }

                    let response = match panic::catch_unwind(panic::AssertUnwindSafe(|| router.handle(cmd))) {
                        Result::Ok(r) => r,
                        Err(_) => Response::internal_error(req_id),
                    };

                    drop(permit);

                    return Some(response);
                    
                },
                RequestOpcode::ChannelConnect => {
                    socket_status.store(SocketStatus::to_u64(on_channel_connect()), Ordering::Relaxed);
                    drop(permit);
                    return Some(Response::channel_connected(req_id));
                },
                RequestOpcode::ChannelMessage => {
                    drop(permit);
                    return Some(Response::bad_request(req_id, Some(Bytes::from("Channel not connected"))));
                }
            }
        },
        SocketStatus::Once => {
            match cmd.headers.opcode {
                RequestOpcode::Once => {
                    let response = match panic::catch_unwind(panic::AssertUnwindSafe(|| router.handle(cmd))) {
                        Result::Ok(r) => r,
                        Err(_) => Response::internal_error(req_id),
                    };

                    drop(permit);

                    return Some(response);
                },
                _ => {
                    drop(permit);
                    return Some(Response::connection_type_mismatch(req_id));
                }
            }
        },
        SocketStatus::Channel(conn_id) => {
            match cmd.headers.opcode {
                RequestOpcode::ChannelConnect => {
                    drop(permit);
                    return Some(Response::conflict(req_id, Bytes::from("Channel already connected")));
                },
                RequestOpcode::ChannelMessage => {
                    match panic::catch_unwind(panic::AssertUnwindSafe(|| router.handle_channel(cmd, &conn_id))) {
                        Result::Ok(Result::Err(r)) => {
                            drop(permit);
                            return Some(r);
                        },
                        Err(_) => {
                            drop(permit);
                            return Some(Response::internal_error(req_id));
                        },
                        _ => {}
                    }
                },
                RequestOpcode::Once => {
                    drop(permit);
                    return Some(Response::connection_type_mismatch(req_id))
                }
            }
        } 
    };

    return None;
}