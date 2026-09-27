use MMA::core::config::Config;
use MMA::core::router::Router;
use MMA::core::server::Server;
use MMA::protocol::commands::RequestOpcode::{Channel, Once};
use MMA::protocol::commands::ResponseOpcode::{
    BadRequest, Conflict, Forbidden, InternalError, Message, NotFound, Unauthorized,
};
use MMA::protocol::commands::{Response, ResponseHeaders, ResponseOpcode};
use MMA::protocol::config::FramerConfig;
use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let mut req_opcodes = [None; 256];
    req_opcodes[1] = Some(Channel);
    req_opcodes[2] = Some(Once);
    let mut res_opcodes = [None; 256];
    res_opcodes[1] = Some(ResponseOpcode::Ok);
    res_opcodes[2] = Some(BadRequest);
    res_opcodes[3] = Some(Conflict);
    res_opcodes[4] = Some(Forbidden);
    res_opcodes[5] = Some(InternalError);
    res_opcodes[6] = Some(Unauthorized);
    res_opcodes[7] = Some(Message);
    res_opcodes[8] = Some(NotFound);
    let config = Config {
        address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080),
        frame_config: FramerConfig {
            response_code_by_opcode: [0,1,3,4,5,6,7,8],
            // request_opcode_code: req_opcodes,
            // response_opcode_code: res_opcodes,
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
            println!("{:?}", &payload);
            let number = match String::from_utf8(payload.to_vec()) {
                Ok(value) => value,
                Err(_) => "2".to_string(),
            }
            .parse::<i32>()
            .unwrap_or(1);
            let pow: u32 = match frame.options.iter().find(|x| x.0 == "pow".to_string()) {
                Some(value) => value.1.parse().unwrap_or_else(|_| 2),
                None => 2,
            };
            let response = format!("{}^{} = {}", number, pow, (number.pow(pow))).to_string();

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
