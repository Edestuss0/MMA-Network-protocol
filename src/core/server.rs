use crate::core::config::Config;
use crate::core::router::Router;
use crate::protocol::framer::{Framer};
use futures_util::{SinkExt, StreamExt};
use std::sync::{mpsc, Arc};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use crate::protocol::commands::Response;

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
            socket.set_nodelay(true)?;

            println!("New connection from {}", address);
            let router = Arc::clone(&router);
            let frame_config = self.config.frame_config.clone();

            tokio::spawn(async move {
                let mut framed = Framed::new(socket, Framer::new(frame_config));
                let (mut sink, mut stream) = framed.split();
                let (tx, mut rx) = tokio::sync::mpsc::channel::<Response>(100);

                tokio::spawn(async move {
                    while let Some(frame) = rx.recv().await {
                        if let Err(e) = sink.send(frame).await {
                            eprintln!("Error sending frame: {}", e);
                            break;
                        }
                    }
                });

               while let Some(frame) = stream.next().await {
                   match frame {
                       Ok(cmd) => {
                           let tx_clone = tx.clone();
                           let router_clone = Arc::clone(&router);
                           tokio::spawn(async move {
                               let response = router_clone.handle(cmd).await;
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
