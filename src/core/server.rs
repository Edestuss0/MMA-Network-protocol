use crate::core::config::Config;
use crate::core::router::Router;
use crate::protocol::commands::Response;
use crate::protocol::framer::Framer;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::codec::Framed;

pub struct Server {
    config: Config,
    tcp_listener: TcpListener,
}

impl Server {
    pub async fn new(config: Config) -> Result<Self, std::io::Error> {
        match config.frame_config.validate() {
            Ok(_) => {}
            Err(_) => {
                eprintln!("Invalid frame config");
                panic!();
            }
        }
        let address = format!("{}", config.address);
        let listener = TcpListener::bind(address).await?;
        Ok(Self {
            config: config,
            tcp_listener: listener,
        })
    }
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, std::io::Error> {
        self.tcp_listener.local_addr()
    }
    pub async fn run(&mut self, router: Router) -> Result<(), std::io::Error> {
        println!("Server listen on {} now", self.config.address);
        let router = Arc::new(router);

        let semaphore = Arc::new(Semaphore::new(self.config.max_in_flight as usize));

        loop {
            let (socket, _) = self.tcp_listener.accept().await?;
            socket.set_nodelay(true)?;
            let router = Arc::clone(&router);
            let frame_config = self.config.frame_config.clone();

            let max_batch = self.config.max_batch;
            let semaphore = Arc::clone(&semaphore);

            tokio::spawn(async move {
                let semaphore = Arc::clone(&semaphore);
                let framed = Framed::new(socket, Framer::new(frame_config));
                let (mut sink, mut stream) = framed.split();
                let (tx, mut rx) = tokio::sync::mpsc::channel::<Response>(1000);

                tokio::spawn(async move {
                    while let Some(frame) = rx.recv().await {

                        if let Err(e) = sink.feed(frame).await {
                            eprintln!("Error sending frame: {}", e);
                            break;
                        }
                        let mut count = 1;
                        while count < max_batch {
                            match rx.try_recv() {
                                Ok(x) => {
                                    if let Err(_) = sink.feed(x).await {
                                        break;
                                    }
                                    count += 1;
                                }
                                Err(_) => break,
                            }
                        }
                        if let Err(e) = sink.flush().await {
                            eprintln!("Error flushing sink: {}", e);
                            break;
                        }
                    }
                });


                while let Some(frame) = stream.next().await {
                    match frame {
                        Ok(cmd) => {
                            let semaphore_clone = Arc::clone(&semaphore);
                            let tx_clone = tx.clone();
                            let router_clone = Arc::clone(&router);
                            let permit = semaphore_clone.acquire_owned().await.unwrap();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let response = router_clone.handle(cmd);
                                if let Err(e) = tx_clone.send(response).await {
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
            });
        }
    }
}
