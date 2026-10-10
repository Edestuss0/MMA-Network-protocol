use crate::core::commands::{Request, Response, ResponseHeaders, ResponseOpcode};
use crate::core::config::MMA_VERSION;
use ahash::AHashMap;
use bytes::Bytes;
use std::sync::Arc;

pub type Handler = Arc<dyn Fn(Request, &mut Response) + Send + Sync>;

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

    fn validate_version(frame: &Request) -> Result<(), Response> {
        if frame.headers.version != MMA_VERSION {
            return Err(Response{
                headers: ResponseHeaders{
                    version: MMA_VERSION,
                    opcode: ResponseOpcode::BadRequest,
                    req_id: frame.headers.req_id
                },
                payload: Bytes::from("Incorrect protocol version"),
                options: Vec::new(),
            })
        }

        return Ok(())
    }

    pub fn handle(&self, frame: Request) -> Response {
        if let Err(e) = Self::validate_version(&frame) {
            return e
        }
        match self.routes.get(&frame.route) {
            Some(handler) => { 
                let mut response = Response::new(&frame.headers.req_id);
                handler(frame, &mut response);
                response
            },
            None => Response {
                headers: ResponseHeaders {
                    version: MMA_VERSION,
                    opcode: ResponseOpcode::NotFound,
                    req_id: frame.headers.req_id,
                },
                payload: Bytes::from("No such method"),
                options: vec![],
            },
        }
    }
}