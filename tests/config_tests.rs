use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use MMA::core::config::{Config, FramerConfig, MIN_HEADER_LEN, RESERVED_SUFFIX};

fn valid_config() -> Config {
    Config::default()
}

#[test]
fn defaults_are_valid() {
    let config = valid_config();
    assert!(config.validate().is_ok());
    assert!(config.frame_config.validate().is_ok());
}

#[test]
fn server_config_rejects_zero_limits_and_port() {
    let mut config = valid_config();
    config.max_connections = 0;
    assert!(config.validate().is_err());
    config = valid_config();
    config.max_batch = 0;
    assert!(config.validate().is_err());
    config = valid_config();
    config.max_in_flight = 0;
    assert!(config.validate().is_err());
    config = valid_config();
    config.address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    assert!(config.validate().is_err());
}

#[test]
fn framer_config_accepts_minimum_header_and_permutations() {
    let mut config = FramerConfig::new();
    config.header_length = MIN_HEADER_LEN + RESERVED_SUFFIX;
    assert!(config.validate().is_ok());
    for positions in [(1, 2, 3, 4), (4, 3, 2, 1), (13, 14, 15, 16)] {
        config.opcode_pos = positions.0;
        config.version_pos = positions.1;
        config.route_len_pos = positions.2;
        config.options_count_pos = positions.3;
        assert!(config.validate().is_ok());
    }
}

#[test]
fn framer_config_rejects_bad_header_positions() {
    let base = FramerConfig::new();
    for mutate in [
        |c: &mut FramerConfig| c.header_length = MIN_HEADER_LEN + RESERVED_SUFFIX - 1,
        |c: &mut FramerConfig| c.opcode_pos = 0,
        |c: &mut FramerConfig| c.version_pos = 0,
        |c: &mut FramerConfig| c.route_len_pos = 0,
        |c: &mut FramerConfig| c.options_count_pos = 0,
        |c: &mut FramerConfig| c.opcode_pos = c.version_pos,
        |c: &mut FramerConfig| c.opcode_pos = c.header_length - RESERVED_SUFFIX + 1,
    ] {
        let mut config = base.clone();
        mutate(&mut config);
        assert!(config.validate().is_err());
    }
}

#[test]
fn framer_config_rejects_bad_body_orders() {
    let base = FramerConfig::new();
    for mutate in [
        |c: &mut FramerConfig| c.route_order = 0,
        |c: &mut FramerConfig| c.payload_order = 0,
        |c: &mut FramerConfig| c.options_order = 0,
        |c: &mut FramerConfig| c.route_order = 4,
        |c: &mut FramerConfig| c.route_order = c.payload_order,
    ] {
        let mut config = base.clone();
        mutate(&mut config);
        assert!(config.validate().is_err());
    }
}
