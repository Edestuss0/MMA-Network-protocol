use crate::core::config::Config;
use crate::core::router::Router;
use crate::protocol::framer::{Framer};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

pub struct Server {
    config: Config,
    tcp_listener: TcpListener,
}

impl Server {
    pub async fn new(config: Config) -> Result<Self, std::io::Error> {
        match config.frame_config.validate() {
            Ok(_) => {},
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
    pub async fn run(&mut self, router: Router) -> Result<(), std::io::Error> {

        println!("Server listen on {} now", self.config.address);
        let router = Arc::new(router);

        loop {
            let (socket, address) = self.tcp_listener.accept().await?;

            println!("New connection from {}", address);
            let router = Arc::clone(&router);
            let frame_config = self.config.frame_config.clone();

            tokio::spawn(async move {
            let mut framed = Framed::new(socket, Framer::new(frame_config));

                while let Some(result) = framed.next().await {
                    match result {
                        Ok(cmd) => {
                            print!("{:?}", cmd.payload);

                            let response = router.handle(cmd).await;

                            match framed.send(response).await {
                                Ok(_) => {
                                    println!("Response sent");
                                }

                                Err(e) => {
                                    eprintln!("Send error: {e}");
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Protocol error: {e}");
                            break;
                        }
                    }
                }
            });
        }
    }
}
