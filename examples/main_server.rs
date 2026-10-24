use MMA::core::commands::{Request, Response};
use MMA::core::config::{Config, FramerConfig};
use MMA::core::router::{self, Router};
use MMA::core::server::{Server, ServerHandle};
use bytes::{Buf, Bytes};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

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
            options_key_first: false,
            header_length: 29,
        },
        max_batch: 65535,
        max_in_flight: 65535,
        max_connections: 128_000,
        drain_timeout: Duration::from_secs(2),
        shutdown_grace: Duration::from_secs(10),
        request_timeout: Duration::from_secs(10),
    };

    let mut router = Router::new();

    let mut server = match Server::new(config).await {
        Ok(value) => value,
        Err(e) => {
            println!("Error: {}", e);
            panic!()
        }
    };

    if router.register_route(String::from("POW"), pow).is_err() {
        eprint!("Invalid route");
        panic!();
    }
    let handle = server.handle();

    if router  .register_channel_handler(
        String::from("SEND"),
        {
            let handle = handle.clone();

            move |req, conn_id| {
                let handle = handle.clone();

                async move {
                    send(req, conn_id, handle).await;
                }
            }
        },
    )
    .is_err()
{
    eprintln!("Invalid route");
    panic!();
}

    let _ = server.run(router).await;
}

async fn pow(req: Request, mut res: Response) -> Response {
    let number = std::str::from_utf8(&req.payload)
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .unwrap_or(2);
    let pow = match req.get_option("pow".as_bytes()) {
        Some(value) => str::from_utf8(&value).unwrap().parse::<u32>().unwrap(),
        None => 2,
    };

    let response = format!("{}^{} = {}", number, pow, (number.pow(pow)));

    res.payload = Bytes::from(response);

    res
}

async fn send(req: Request, conn_id: u32, handle: ServerHandle) {
    let payload = req.payload.clone();

    let mut response = Response::new(0);
    response.payload = payload;

    let _ = handle.send_to_channel(0, response);
}
