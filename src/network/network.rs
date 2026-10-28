use crate::core::commands::ChannelSendedMessage;
use crate::core::commands::ProtocolError;
use crate::core::commands::Request;
use crate::core::commands::RequestOpcode;
use crate::core::commands::Response;
use crate::core::config::Config;
use crate::core::router::Router;
use crate::protocol::framer::Framer;
use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tokio::task::JoinHandle;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug, Clone, Copy)]
pub enum SocketStatus {
    Fresh,
    Once,
    Channel(u32),
}

impl SocketStatus {
    fn to_u64(self) -> u64 {
        match self {
            SocketStatus::Fresh => 0,
            SocketStatus::Once => 1_u64 << 32,
            SocketStatus::Channel(id) => (2_u64 << 32) | u64::from(id),
        }
    }

    fn from_u64(value: u64) -> Self {
        match value >> 32 {
            0 => SocketStatus::Fresh,
            1 => SocketStatus::Once,
            2 => SocketStatus::Channel(value as u32),
            _ => SocketStatus::Fresh,
        }
    }
}

pub async fn serve_connection<F>(
    socket: TcpStream,
    router: Arc<Router>,
    mut config_receiver: watch::Receiver<Config>,
    semaphore: Arc<Semaphore>,
    stop: CancellationToken,
    kill: CancellationToken,
    conn_permit: OwnedSemaphorePermit,
    
    on_channel_connect: F,
) where
    F: Fn() -> (SocketStatus, broadcast::Receiver<ChannelSendedMessage>) + Send + Sync + 'static,
{
    let mut config = config_receiver.borrow_and_update().clone();

    let framer_config = Arc::new(RwLock::new(config.frame_config.clone()));

    let framed = Framed::new(socket, Framer::new(Arc::clone(&framer_config)));

    let (sink, mut stream) = framed.split();
    let (tx, rx) = tokio::sync::mpsc::channel::<Response>(1000);
    let inflight = TaskTracker::new();

    let conn_semaphore = Arc::new(Semaphore::new(256));
    let tx_copy = tx.clone();

    let on_channel_connect = Arc::new(on_channel_connect);
    let socket_status = Arc::new(AtomicU64::new(SocketStatus::to_u64(SocketStatus::Fresh)));
    let channel_task: Arc<Mutex<Option<JoinHandle<()>>>> = Arc::new(Mutex::new(None));

    let mut writer = writer(sink, rx, config.max_batch);

    loop {
        let frame = tokio::select! {
            biased;
            _ = kill.cancelled() => break,
            _ = stop.cancelled() => break,
            changed = config_receiver.changed() => {
                if changed.is_err() {
                    break;
                }

                let next_config = config_receiver
                    .borrow_and_update()
                    .clone();

                {
                    let mut current = framer_config
                        .write()
                        .expect("Framer config lock poisoned");

                    *current = next_config.frame_config.clone();
                }

                config = next_config;
                continue;
            }
            f = stream.next() => match f { Some(f) => f, None => break},
        };

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
                let on_channel_connect = Arc::clone(&on_channel_connect);
                let socket_status = Arc::clone(&socket_status);
                let req_id = cmd.headers.req_id;

                let request_timeout = config.request_timeout;

                let channel_task_for_cmd = Arc::clone(&channel_task);
                let tx_for_channel = tx_copy.clone();

                inflight.spawn(async move {
                    match tokio::time::timeout(
                        request_timeout,
                        handle_cmd(
                            cmd,
                            socket_status,
                            permit_global,
                            router,
                            Arc::new(move || -> SocketStatus {
                                let (status, mut receiver) = on_channel_connect();

                                let conn_id = match status {
                                    SocketStatus::Channel(id) => id,
                                    _ => return status,
                                };

                                let tx_channel = tx_for_channel.clone();

                                let relay_task = tokio::spawn(async move {
                                    loop {
                                        match receiver.recv().await {
                                            Ok(message) => {
                                                if message.conn_id == conn_id || message.conn_id == 0 {
                                                    if tx_channel.send(message.message).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }

                                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                                eprintln!(
                                                    "Channel receiver lagged; skipped {skipped} messages"
                                                );
                                            }

                                            Err(broadcast::error::RecvError::Closed) => {
                                                break;
                                            }
                                        }
                                    }
                                });

                                match channel_task_for_cmd.lock() {
                                    Ok(mut slot) => {
                                        if let Some(previous) = slot.replace(relay_task) {
                                            previous.abort();
                                        }
                                    }

                                    Err(_) => {
                                        relay_task.abort();
                                    }
                                }

                                status
                            })
                        ),
                    )
                    .await
                    {
                        Ok(r) => {
                            if r.is_some()
                                && let Err(e) = tx.send(r.unwrap()).await {
                                    eprintln!("{}", e);
                                }
                        }
                        Err(_) => {
                            if let Err(e) = tx.send(Response::timeout(req_id)).await {
                                eprintln!("{}", e);
                            }
                        }
                    };
                    drop(permit_local)
                });
            }
            Err(error) => {
                if let Err(e) = tx
                    .send(Response::bad_request(
                        match error {
                            ProtocolError::Io(_) => 0,
                            ProtocolError::MessageTooLarge { message: _, req_id } => {
                                req_id.unwrap_or_default()
                            }
                            ProtocolError::InvalidFrame { message: _, req_id } => {
                                req_id.unwrap_or_default()
                            }
                            ProtocolError::InvalidMessageType { message: _, req_id } => {
                                req_id.unwrap_or_default()
                            }
                            ProtocolError::InvalidUtf8 { req_id } => req_id.unwrap_or_default(),
                        },
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
    if let Ok(mut slot) = channel_task.lock() {
        if let Some(task) = slot.take() {
            task.abort();
        }
    }
    if kill.is_cancelled() {
        writer.abort();
        let _ = writer.await;
    } else {
        if let Err(_) = tokio::time::timeout(config.drain_timeout, &mut writer).await {
            writer.abort();
            let _ = writer.await;
        }
        let _ = tokio::time::timeout(config.drain_timeout, async {
            while let Some(Result::Ok(_)) = stream.next().await {}
        })
        .await;
    }
}

async fn handle_cmd<F>(
    cmd: Request,
    socket_status: Arc<AtomicU64>,
    permit: OwnedSemaphorePermit,
    router: Arc<Router>,
    on_channel_connect: Arc<F>,
) -> Option<Response>
where
    F: Fn() -> SocketStatus + Send + Sync + 'static,
{
    let req_id = cmd.headers.req_id;

    if let Err(e) = Router::validate_version(&cmd) {
        return Some(e);
    }

    match SocketStatus::from_u64(socket_status.load(Ordering::Relaxed)) {
        SocketStatus::Fresh => match cmd.headers.opcode {
            RequestOpcode::Once => {
                socket_status.store(SocketStatus::to_u64(SocketStatus::Once), Ordering::Relaxed);
                let response = match AssertUnwindSafe(router.handle(cmd)).catch_unwind().await {
                    Ok(response) => response,
                    Err(_) => Response::internal_error(req_id),
                };

                drop(permit);

                return Some(response);
            }
            RequestOpcode::ChannelConnect => {
                socket_status.store(
                    SocketStatus::to_u64(on_channel_connect()),
                    Ordering::Relaxed,
                );
                drop(permit);
                return Some(Response::channel_connected(req_id));
            }
            RequestOpcode::ChannelMessage => {
                drop(permit);
                return Some(Response::bad_request(
                    req_id,
                    Some(Bytes::from("Channel not connected")),
                ));
            }
        },
        SocketStatus::Once => match cmd.headers.opcode {
            RequestOpcode::Once => {
                let response = match AssertUnwindSafe(router.handle(cmd)).catch_unwind().await {
                    Ok(response) => response,
                    Err(_) => Response::internal_error(req_id),
                };

                drop(permit);

                return Some(response);
            }
            _ => {
                drop(permit);
                return Some(Response::connection_type_mismatch(req_id));
            }
        },
        SocketStatus::Channel(conn_id) => match cmd.headers.opcode {
            RequestOpcode::ChannelConnect => {
                drop(permit);
                return Some(Response::conflict(
                    req_id,
                    Bytes::from("Channel already connected"),
                ));
            }
            RequestOpcode::ChannelMessage => {
                match AssertUnwindSafe(router.handle_channel(cmd, &conn_id))
                    .catch_unwind()
                    .await
                {
                    Result::Ok(Result::Err(r)) => {
                        drop(permit);
                        return Some(r);
                    }
                    Err(_) => {
                        drop(permit);
                        return Some(Response::internal_error(req_id));
                    }
                    _ => {}
                }
            }
            RequestOpcode::Once => {
                drop(permit);
                return Some(Response::connection_type_mismatch(req_id));
            }
        },
    };

    None
}

fn writer(
    mut sink: SplitSink<Framed<TcpStream, Framer>, Response>,
    mut rx: Receiver<Response>,
    max_batch: u32,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if let Err(e) = sink.feed(frame).await {
                match e {
                    ProtocolError::MessageTooLarge { message: _, req_id } => {
                        if let Err(err) = sink
                            .feed(Response::internal_error(req_id.unwrap_or_default()))
                            .await
                        {
                            eprintln!("Error sending frame: {}", err);
                        }
                    }
                    ProtocolError::InvalidFrame { message: _, req_id } => {
                        if let Err(err) = sink
                            .feed(Response::internal_error(req_id.unwrap_or_default()))
                            .await
                        {
                            eprintln!("Error sending frame: {}", err);
                        }
                    }
                    ProtocolError::InvalidMessageType { message: _, req_id } => {
                        if let Err(err) = sink
                            .feed(Response::internal_error(req_id.unwrap_or_default()))
                            .await
                        {
                            eprintln!("Error sending frame: {}", err);
                        }
                    }
                    ProtocolError::InvalidUtf8 { req_id } => {
                        if let Err(err) = sink
                            .feed(Response::internal_error(req_id.unwrap_or_default()))
                            .await
                        {
                            eprintln!("Error sending frame: {}", err);
                        }
                    }
                    _ => {
                        eprintln!("Error sending frame: {}", e);
                    }
                }
                return;
            }
            let mut count = 1;
            while count < max_batch {
                match rx.try_recv() {
                    Result::Ok(x) => {
                        if let Err(e) = sink.feed(x).await {
                            match e {
                                ProtocolError::MessageTooLarge { message: _, req_id } => {
                                    if let Err(err) = sink
                                        .feed(Response::internal_error(req_id.unwrap_or_default()))
                                        .await
                                    {
                                        eprintln!("Error sending frame: {}", err);
                                    }
                                }
                                ProtocolError::InvalidFrame { message: _, req_id } => {
                                    if let Err(err) = sink
                                        .feed(Response::internal_error(req_id.unwrap_or_default()))
                                        .await
                                    {
                                        eprintln!("Error sending frame: {}", err);
                                    }
                                }
                                ProtocolError::InvalidMessageType { message: _, req_id } => {
                                    if let Err(err) = sink
                                        .feed(Response::internal_error(req_id.unwrap_or_default()))
                                        .await
                                    {
                                        eprintln!("Error sending frame: {}", err);
                                    }
                                }
                                ProtocolError::InvalidUtf8 { req_id } => {
                                    if let Err(err) = sink
                                        .feed(Response::internal_error(req_id.unwrap_or_default()))
                                        .await
                                    {
                                        eprintln!("Error sending frame: {}", err);
                                    }
                                }
                                _ => {
                                    eprintln!("Error sending frame: {}", e);
                                }
                            }
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
    })
}
