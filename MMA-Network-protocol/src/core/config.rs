use crate::protocol::config::FramerConfig;
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct Config {
    pub address: SocketAddr,
    pub frame_config: FramerConfig,
    pub max_batch: u8
}

pub const MMA_VERSION: u8 = 1;