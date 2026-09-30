use MMA::core::config::Config;
use MMA::core::router::Router;
use MMA::core::server::Server;
use MMA::protocol::commands::{Response, ResponseHeaders, ResponseOpcode};
use MMA::protocol::config::FramerConfig;
use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

#[tokio::main]
async fn main() {   
    let config = Config {
        address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080),
        frame_config: FramerConfig {
            opcode_pos: 1,
            version_pos: 4,
            max_message_length: 10 * 1000 * 1000,
            route_len_pos: 6,
            options_count_pos: 7,
            route_order: 3,
            options_order: 2,
            payload_order: 1,
            options_key_first: true,
            header_length: 29,
        },
        max_batch: 255
    };

    let mut router = Router::new();

    let mut server = match Server::new(config).await {
        Ok(value) => value,
        Err(e) => {
            println!("Error: {}", e);
            panic!()
        }
    };

    router.register_route(
        "GET".to_string(),
        Arc::new(move |request| {
            return Response {
                options: vec![],
                payload: request.payload,
                headers: ResponseHeaders {
                    opcode: ResponseOpcode::Ok,
                    version: 1,
                    req_id: request.headers.req_id,
                },
            };
        }),
    );

    router.register_route(
        "POW".to_string(),
        Arc::new(move |frame| {
            let mut payload = frame.payload.clone();
            let number = match std::str::from_utf8(&payload) {
                Ok(value) => value,
                Err(_) => "2"
            }
            .parse::<i32>()
            .unwrap_or(1);
            let pow: u32 = match frame.options.iter().find(|x| x.0 == "pow") {
                Some(value) => value.1.parse().unwrap_or_else(|_| 2),
                None => 2,
            };
            let response = format!("{}^{} = {}", number, pow, (number.pow(pow)));

            return Response {
                headers: ResponseHeaders {
                    opcode: ResponseOpcode::Ok,
                    version: 1,
                    req_id: frame.headers.req_id,
                },
                payload: Bytes::from(response),
                options: vec![],
            };
        }),
    );

    server.run(router).await;
}
