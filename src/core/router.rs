use crate::core::commands::{Request, Response, ResponseHeaders, ResponseOpcode};
use crate::core::config::MMA_VERSION;
use ahash::AHashMap;
use bytes::Bytes;
use std::str::FromStr;
use std::sync::Arc;
use strum_macros::EnumString;

pub type Handler = Arc<dyn Fn(Request, &mut Response) + Send + Sync>;

pub type ChannelHandler = Arc<dyn Fn(Request, &u32) + Send + Sync>;

#[derive(Debug, PartialEq, EnumString)]
enum ReservedRoutes {
    CHANNEL_CONNECT,
}

#[derive(Clone)]
pub struct Router {
    routes: AHashMap<String, Handler>,
    channel_handlers: AHashMap<String, ChannelHandler>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            routes: AHashMap::new(),
            channel_handlers: AHashMap::new(),
        }
    }
    pub fn register_route(&mut self, route: String, handler: Handler) -> Result<(), ()> {
        if ReservedRoutes::from_str(route.to_uppercase().as_str()).is_ok() {
            return Err(());
        }
        self.routes.insert(route, handler);
        Result::Ok(())
    }

    pub fn register_channel_handler(
        &mut self,
        route: String,
        handler: ChannelHandler,
    ) -> Result<(), ()> {
        if ReservedRoutes::from_str(route.to_uppercase().as_str()).is_ok() {
            return Err(());
        }
        self.channel_handlers.insert(route, handler);
        Result::Ok(())
    }

    fn validate_version(frame: &Request) -> Result<(), Response> {
        if frame.headers.version != MMA_VERSION {
            return Err(Response {
                headers: ResponseHeaders {
                    version: MMA_VERSION,
                    opcode: ResponseOpcode::BadRequest,
                    req_id: frame.headers.req_id,
                },
                payload: Bytes::from("Incorrect protocol version"),
                options: Vec::new(),
            });
        }

        Result::Ok(())
    }

    pub fn handle(&self, frame: Request) -> Response {
        if let Err(e) = Self::validate_version(&frame) {
            return e;
        }
        match self.routes.get(&frame.route) {
            Some(handler) => {
                let mut response = Response::new(frame.headers.req_id);
                handler(frame, &mut response);
                response
            }
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

    pub fn handle_channel(&self, frame: Request, conn_id: &u32) -> Result<(), Response> {
        Self::validate_version(&frame)?;
        match self.channel_handlers.get(&frame.route) {
            Some(handler) => {
                handler(frame, conn_id);
                Result::Ok(())
            }
            None => Err(Response {
                headers: ResponseHeaders {
                    version: MMA_VERSION,
                    opcode: ResponseOpcode::NotFound,
                    req_id: frame.headers.req_id,
                },
                payload: Bytes::from("No such method"),
                options: vec![],
            }),
        }
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}
