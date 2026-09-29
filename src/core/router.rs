use crate::protocol::commands::{Response, ResponseHeaders};
use crate::protocol::framer::{Request, ResponseOpcode};
use bytes::Bytes;
use std::sync::Arc;
use ahash::AHashMap;

pub type Handler = Arc<dyn Fn(Request) -> Response + Send + Sync>;

#[derive(Clone)]
pub struct Router {
    routes: AHashMap<String, Handler>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            routes: AHashMap::new(),
        }
    }
    pub fn register_route(&mut self, route: String, handler: Handler) {
        self.routes.insert(route, handler);
    }

    pub fn handle(&self, frame: Request) -> Response {
        match self.routes.get(&frame.route) {
            Some(handler) => handler(frame),
            None => Response {
                headers: ResponseHeaders {
                    version: 1,
                    opcode: ResponseOpcode::NotFound,
                    req_id: frame.headers.req_id,
                },
                payload: Bytes::from("No such method"),
                options: vec![],
            },
        }
    }
}