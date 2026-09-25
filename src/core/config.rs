use std::net::SocketAddr;
use crate::protocol::config::FramerConfig;

#[derive(Debug, Clone)]
pub struct Config {
    pub address: SocketAddr,
    pub frame_config: FramerConfig,
}
