use crate::core::commands::{Request, Response, ResponseHeaders, ResponseOpcode};
use crate::core::config::MMA_VERSION;
use crate::core::router::RouteError::{Duplicate, InvalidName};
use ahash::AHashMap;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use std::str::FromStr;
use std::sync::Arc;
use strum_macros::EnumString;

type Handler = Arc<dyn for<'a> Fn(Request, Response) -> BoxFuture<'static, Response> + Send + Sync>;
type ChannelHandler = Arc<dyn Fn(Request, u32) -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Debug, PartialEq, EnumString)]
enum ReservedRoutes {
    CHANNEL_CONNECT,
}

#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    #[error("route name is empty or too long")]
    InvalidName,
    #[error("route is reserved")]
    Reserved,
    #[error("route already exists")]
    Duplicate,
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
    pub fn register_route<F, Fut>(&mut self, route: String, handler: F) -> Result<(), RouteError>
    where
        F: Fn(Request, Response) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        if route.is_empty() || route.len() > u8::MAX as usize {
            return Err(InvalidName);
        }

        if ReservedRoutes::from_str(&route.to_uppercase()).is_ok() {
            return Err(RouteError::Reserved);
        }

        if self.routes.contains_key(&route) || self.channel_handlers.contains_key(&route) {
            return Err(Duplicate);
        }

        let handler: Handler = Arc::new(move |req, res| -> BoxFuture<'static, Response> {
            Box::pin(handler(req, res))
        });

        self.routes.insert(route, handler);
        Ok(())
    }

    pub fn register_channel_handler<F, Fut>(
        &mut self,
        route: String,
        handler: F,
    ) -> Result<(), RouteError>
    where
        F: Fn(Request, u32) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if route.is_empty() || route.len() > u8::MAX as usize {
            return Err(InvalidName);
        }

        if ReservedRoutes::from_str(&route.to_uppercase()).is_ok() {
            return Err(RouteError::Reserved);
        }

        if self.routes.contains_key(&route) || self.channel_handlers.contains_key(&route) {
            return Err(Duplicate);
        }

        let handler: ChannelHandler = Arc::new(move |req, conn_id| -> BoxFuture<'static, ()> {
            Box::pin(handler(req, conn_id))
        });

        self.channel_handlers.insert(route, handler);

        Result::Ok(())
    }

    pub fn validate_version(frame: &Request) -> Result<(), Response> {
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

    pub async fn handle(&self, frame: Request) -> Response {
        if let Err(e) = Self::validate_version(&frame) {
            return e;
        }
        match self.routes.get(&frame.route) {
            Some(handler) => {
                let response = Response::new(frame.headers.req_id);
                handler(frame, response).await
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

    pub async fn handle_channel(&self, frame: Request, conn_id: &u32) -> Result<(), Response> {
        Self::validate_version(&frame)?;
        match self.channel_handlers.get(&frame.route) {
            Some(handler) => {
                handler(frame, *conn_id).await;
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
