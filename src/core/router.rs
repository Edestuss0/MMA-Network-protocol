use std::collections::HashMap;
use std::sync::Arc;
use bytes::Bytes;
use crate::protocol::commands::{Response, ResponseHeaders};
use crate::protocol::framer::{Request, ResponseOpcode};

pub type Handler = Arc<dyn Fn(Request) -> Response + Send + Sync>;

#[derive(Clone)]
pub struct Router {
    routes: HashMap<String, Handler>,
}

impl Router {

    pub fn new() -> Self {
        Self{routes: HashMap::new()}
    }
    pub fn register_route(&mut self, route: String, handler: Handler) {
        self.routes.insert(route, handler);
    }

    pub fn handle(&self, frame: Request) -> Response {
        match self.routes.get(&frame.route) {
            Some(handler) => {
                handler(frame)
            }
            None => {
                Response{headers: ResponseHeaders{version: 1, opcode: ResponseOpcode::NotFound,}, payload: Bytes::from("No such method"), options: vec![]}
            }
        }
    }
}