use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use crate::core::config::ConfigError::{MaxBatch, MaxConnections, MaxInFlight, Port};

#[derive(Debug, Clone)]
pub struct Config {
    pub address: SocketAddr,
    pub frame_config: FramerConfig,
    pub max_batch: u32,
    pub max_in_flight: u32,
    pub max_connections: u32,
    pub drain_timeout: Duration,
    pub shutdown_grace: Duration,
    pub request_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("max_connections must be > 0")]
    MaxConnections,
    #[error("max_in_flight must be > 0")]
    MaxInFlight,
    #[error("max_batch must be > 0")]
    MaxBatch,
    #[error("invalid framer config")]
    Framer,
    #[error("invalid port")]
    Port,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080),
            frame_config: FramerConfig::default(),
            max_batch: 255,
            max_in_flight: 1024,
            max_connections: 128_000,
            drain_timeout: Duration::from_secs(2),
            shutdown_grace: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10),
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_connections == 0 {
            return Err(MaxConnections);
        }

        if self.max_batch == 0 {
            return Err(MaxBatch);
        }

        if self.max_in_flight == 0 {
            return Err(MaxInFlight);
        }

        if self.address.port() == 0 {
            return Err(Port);
        }

        if self.frame_config.validate().is_err() {
            return Err(ConfigError::Framer);
        }

        Result::Ok(())
    }
}

pub const MMA_VERSION: u8 = 1;

#[derive(Debug, Clone)]
pub struct FramerConfig {
    pub max_message_length: u32,
    pub opcode_pos: u8,
    pub version_pos: u8,
    pub route_len_pos: u8,
    pub route_order: u8,
    pub payload_order: u8,
    pub options_order: u8,
    pub options_count_pos: u8,
    pub options_key_first: bool,
    pub header_length: u8,
    pub big_bytes_config: BigBytesConfig
}

#[derive(Debug, Clone)]
pub struct BigBytesConfig {
    pub payload_len_order: u8,
    pub options_len_order: u8,
    pub req_id_order: u8
}

impl BigBytesConfig {
    pub fn new() -> Self {
        Self{
            options_len_order: 1,
            payload_len_order: 2,
            req_id_order: 3
        }
    }
}

pub const MIN_HEADER_LEN: u8 = 17;
pub const RESERVED_SUFFIX: u8 = 12;
impl FramerConfig {
    pub fn validate(&self) -> Result<(), ()> {
        if self.header_length < MIN_HEADER_LEN {
            return Err(());
        }
        let poses = [
            self.opcode_pos,
            self.version_pos,
            self.route_len_pos,
            self.options_count_pos,
        ];
        if poses.contains(&0) {
            return Err(());
        }
        for item in poses {
            if poses.iter().filter(|&&x| x == item).count() > 1
                || item > (self.header_length - RESERVED_SUFFIX)
            {
                return Err(());
            }
        }

        let orders = [self.payload_order, self.options_order, self.route_order];
        if orders.contains(&0) {
            return Err(());
        }
        for item in orders {
            if orders.iter().filter(|&&x| x == item).count() > 1 || item > 3 {
                return Err(());
            }
        }

        Result::Ok(())
    }
}

impl Default for FramerConfig {
    fn default() -> Self {
        Self {
            opcode_pos: 1,
            version_pos: 2,
            max_message_length: 10 * 1000 * 1000,
            route_len_pos: 3,
            options_count_pos: 4,
            route_order: 1,
            options_order: 2,
            payload_order: 3,
            options_key_first: true,
            header_length: MIN_HEADER_LEN + RESERVED_SUFFIX,
            big_bytes_config: BigBytesConfig::new()
        }
    }
}
