#![allow(non_camel_case_types)]

use crate::core::commands::{Request, Response, ResponseOpcode};
use crate::core::config::{Config, FramerConfig};
use crate::core::router::Router;
use crate::core::server::Server;
use std::ffi::{CStr, c_char, c_void};
use std::net::{IpAddr, SocketAddr, TcpListener};
use std::ptr;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use tokio::sync::oneshot;

pub const MMA_OK: i32 = 0;
pub const MMA_INVALID_ARGUMENT: i32 = -1;
pub const MMA_INVALID_CONFIG: i32 = -2;
pub const MMA_SERVER_ERROR: i32 = -3;
pub const MMA_SERVER_ALREADY_STARTED: i32 = -4;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MMA_FramerConfig {
    pub max_message_length: u32,
    pub opcode_pos: u8,
    pub version_pos: u8,
    pub route_len_pos: u8,
    pub route_order: u8,
    pub payload_order: u8,
    pub options_order: u8,
    pub options_count_pos: u8,
    pub options_key_first: u8,
    pub header_length: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MMA_ServerConfig {
    pub bind_ip: *const c_char,
    pub port: u16,
    pub max_batch: u8,
    pub max_in_flight: u32,
    pub framer: MMA_FramerConfig,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MMA_Option {
    pub key: *const u8,
    pub key_len: usize,
    pub value: *const u8,
    pub value_len: usize,
}

#[repr(C)]
pub struct MMA_Request {
    pub route: *const u8,
    pub route_len: usize,
    pub opcode: u8,
    pub version: u8,
    pub request_id: u32,
    pub payload: *const u8,
    pub payload_len: usize,
    pub options: *const MMA_Option,
    pub options_count: usize,
}

#[repr(C)]
pub struct MMA_Response {
    response: *mut Response,
}

pub type MMA_RouteCallback =
    Option<extern "C" fn(*const MMA_Request, *mut MMA_Response, *mut c_void) -> i32>;

struct RegisteredRoute {
    route: String,
    callback: extern "C" fn(*const MMA_Request, *mut MMA_Response, *mut c_void) -> i32,
    user_data: usize,
}

pub struct MMA_Server {
    config: Config,
    bound_port: u16,
    routes: Vec<RegisteredRoute>,
    started: bool,
}

fn framer_config_from_c(config: MMA_FramerConfig) -> Option<FramerConfig> {
    if config.options_key_first > 1 {
        return None;
    }
    Some(FramerConfig {
        max_message_length: config.max_message_length,
        opcode_pos: config.opcode_pos,
        version_pos: config.version_pos,
        route_len_pos: config.route_len_pos,
        route_order: config.route_order,
        payload_order: config.payload_order,
        options_order: config.options_order,
        options_count_pos: config.options_count_pos,
        options_key_first: config.options_key_first != 0,
        header_length: config.header_length,
    })
}

fn framer_config_to_c(config: &FramerConfig) -> MMA_FramerConfig {
    MMA_FramerConfig {
        max_message_length: config.max_message_length,
        opcode_pos: config.opcode_pos,
        version_pos: config.version_pos,
        route_len_pos: config.route_len_pos,
        route_order: config.route_order,
        payload_order: config.payload_order,
        options_order: config.options_order,
        options_count_pos: config.options_count_pos,
        options_key_first: u8::from(config.options_key_first),
        header_length: config.header_length,
    }
}

fn valid_slice<T>(data: *const T, len: usize) -> bool {
    len == 0 || !data.is_null()
}

unsafe fn raw_slice<'a, T>(data: *const T, len: usize) -> &'a [T] {
    if len == 0 {
        &[]
    } else {
        // The C caller must provide a readable allocation of `len` elements.
        unsafe { std::slice::from_raw_parts(data, len) }
    }
}

fn response_opcode(opcode: u8) -> Option<ResponseOpcode> {
    match opcode {
        1 => Some(ResponseOpcode::Ok),
        2 => Some(ResponseOpcode::BadRequest),
        3 => Some(ResponseOpcode::Unauthorized),
        4 => Some(ResponseOpcode::Forbidden),
        5 => Some(ResponseOpcode::NotFound),
        6 => Some(ResponseOpcode::Conflict),
        7 => Some(ResponseOpcode::InternalError),
        8 => Some(ResponseOpcode::Message),
        _ => None,
    }
}

fn router_from_routes(routes: &[RegisteredRoute]) -> Router {
    let mut router = Router::new();
    for route in routes {
        let callback = route.callback;
        let route_name = route.route.clone();
        let user_data = route.user_data;
        router.register_route(
            route.route.clone(),
            Arc::new(move |request: Request, response: &mut Response| {
                let options: Vec<MMA_Option> = request
                    .options
                    .iter()
                    .map(|(key, value)| MMA_Option {
                        key: key.as_ptr(),
                        key_len: key.len(),
                        value: value.as_ptr(),
                        value_len: value.len(),
                    })
                    .collect();
                let request_view = MMA_Request {
                    route: route_name.as_ptr(),
                    route_len: route_name.len(),
                    opcode: request.headers.opcode as u8,
                    version: request.headers.version,
                    request_id: request.headers.req_id,
                    payload: request.payload.as_ptr(),
                    payload_len: request.payload.len(),
                    options: if options.is_empty() {
                        ptr::null()
                    } else {
                        options.as_ptr()
                    },
                    options_count: options.len(),
                };
                let mut response_view = MMA_Response {
                    response: response as *mut Response,
                };
                let status = callback(&request_view, &mut response_view, user_data as *mut c_void);
                if status != MMA_OK {
                    eprintln!("MMA route {route_name} callback returned status {status}");
                    response.headers.opcode = ResponseOpcode::InternalError;
                    response.payload = bytes::Bytes::new();
                    response.options.clear();
                }
            }),
        );
    }
    router
}

unsafe fn response_mut<'a>(response: *mut MMA_Response) -> Result<&'a mut Response, i32> {
    if response.is_null() || unsafe { (*response).response.is_null() } {
        return Err(MMA_INVALID_ARGUMENT);
    }
    Ok(unsafe { &mut *(*response).response })
}

#[unsafe(no_mangle)]
pub extern "C" fn mma_server_config_default(output: *mut MMA_ServerConfig) -> i32 {
    if output.is_null() {
        return MMA_INVALID_ARGUMENT;
    }
    let framer = FramerConfig::new();
    let config = MMA_ServerConfig {
        bind_ip: c"0.0.0.0".as_ptr(),
        port: 8080,
        max_batch: 16,
        max_in_flight: 1024,
        framer: framer_config_to_c(&framer),
    };
    unsafe {
        *output = config;
    }
    MMA_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_server_create(
    config: *const MMA_ServerConfig,
    output: *mut *mut MMA_Server,
) -> i32 {
    if output.is_null() {
        return MMA_INVALID_ARGUMENT;
    }
    unsafe {
        *output = ptr::null_mut();
    }
    if config.is_null() {
        return MMA_INVALID_ARGUMENT;
    }
    let config = unsafe { *config };
    if config.bind_ip.is_null() {
        return MMA_INVALID_ARGUMENT;
    }
    let bind_ip = match unsafe { CStr::from_ptr(config.bind_ip) }.to_str() {
        Ok(value) => value,
        Err(_) => return MMA_INVALID_ARGUMENT,
    };
    let ip = match bind_ip.parse::<IpAddr>() {
        Ok(value) => value,
        Err(_) => return MMA_INVALID_ARGUMENT,
    };
    let Some(frame_config) = framer_config_from_c(config.framer) else {
        return MMA_INVALID_CONFIG;
    };
    if frame_config.validate().is_err() || config.max_in_flight == 0 {
        return MMA_INVALID_CONFIG;
    }

    let address = SocketAddr::new(ip, config.port);;
    let server = MMA_Server {
        config: Config {
            address,
            frame_config,
            max_batch: config.max_batch,
            max_in_flight: config.max_in_flight,
        },
        bound_port: address.port(),
        routes: Vec::new(),
        started: false,
    };
    unsafe {
        *output = Box::into_raw(Box::new(server));
    }
    MMA_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_server_route(
    server: *mut MMA_Server,
    route: *const u8,
    route_len: usize,
    callback: MMA_RouteCallback,
    user_data: *mut c_void,
) -> i32 {
    if server.is_null() || !valid_slice(route, route_len) {
        return MMA_INVALID_ARGUMENT;
    }
    let server = unsafe { &mut *server };
    if server.started {
        return MMA_SERVER_ALREADY_STARTED;
    }
    if route_len == 0 || route_len > u8::MAX as usize {
        return MMA_INVALID_ARGUMENT;
    }
    let route = match std::str::from_utf8(unsafe { raw_slice(route, route_len) }) {
        Ok(value) => value,
        Err(_) => return MMA_INVALID_ARGUMENT,
    };
    let Some(callback) = callback else {
        return MMA_INVALID_ARGUMENT;
    };
    server.routes.push(RegisteredRoute {
        route: route.to_owned(),
        callback,
        user_data: user_data as usize,
    });
    MMA_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_server_start(server: *mut MMA_Server) -> i32 {
    if server.is_null() {
        return MMA_INVALID_ARGUMENT;
    }
    let server = unsafe { &mut *server };
    if server.started {
        return MMA_SERVER_ALREADY_STARTED;
    }
    let config = server.config.clone();
    let router = router_from_routes(&server.routes);

    let _ = match thread::Builder::new()
        .name("mma-server".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(_) => {
                    return;
                }
            };
            runtime.block_on(async move {
                let mut server = match Server::new(config).await {
                    Ok(server) => server,
                    Err(_) => {
                        return;
                    }
                };
                if let Err(error) = server.run(router).await
                {
                    eprintln!("MMA server stopped after an error: {error}");
                }
            });
        }) {
        Ok(thread) => thread,
        Err(_) => return MMA_SERVER_ERROR,
    };

    MMA_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_server_local_port(server: *const MMA_Server, port: *mut u16) -> i32 {
    if server.is_null() || port.is_null() {
        return MMA_INVALID_ARGUMENT;
    }
    unsafe {
        *port = (*server).bound_port;
    }
    MMA_OK
}
//
// #[unsafe(no_mangle)]
// pub unsafe extern "C" fn mma_server_stop(server: *mut MMA_Server) -> i32 {
//     if server.is_null() {
//         return MMA_INVALID_ARGUMENT;
//     }
//     let server = unsafe { &mut *server };
//     let Some(running) = server.running.take() else {
//         return MMA_OK;
//     };
//     let _ = running.shutdown.send(());
//     if running.thread.join().is_err() {
//         return MMA_SERVER_ERROR;
//     }
//     MMA_OK
// }

// #[unsafe(no_mangle)]
// pub unsafe extern "C" fn mma_server_destroy(server: *mut MMA_Server) {
//     if !server.is_null() {
//         let _ = unsafe { mma_server_stop(server) };
//         drop(unsafe { Box::from_raw(server) });
//     }
// }

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_response_set_status(response: *mut MMA_Response, opcode: u8) -> i32 {
    let Some(opcode) = response_opcode(opcode) else {
        return MMA_INVALID_ARGUMENT;
    };
    let response = match unsafe { response_mut(response) } {
        Ok(response) => response,
        Err(status) => return status,
    };
    response.headers.opcode = opcode;
    MMA_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_response_set_payload(
    response: *mut MMA_Response,
    data: *const u8,
    data_len: usize,
) -> i32 {
    if !valid_slice(data, data_len) {
        return MMA_INVALID_ARGUMENT;
    }
    let response = match unsafe { response_mut(response) } {
        Ok(response) => response,
        Err(status) => return status,
    };
    response.payload = bytes::Bytes::copy_from_slice(unsafe { raw_slice(data, data_len) });
    MMA_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_response_add_option(
    response: *mut MMA_Response,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    if !valid_slice(key, key_len)
        || !valid_slice(value, value_len)
        || key_len == 0
        || value_len == 0
        || key_len > u16::MAX as usize
        || value_len > u16::MAX as usize
    {
        return MMA_INVALID_ARGUMENT;
    }
    let key = match std::str::from_utf8(unsafe { raw_slice(key, key_len) }) {
        Ok(value) => value,
        Err(_) => return MMA_INVALID_ARGUMENT,
    };
    let value = match std::str::from_utf8(unsafe { raw_slice(value, value_len) }) {
        Ok(value) => value,
        Err(_) => return MMA_INVALID_ARGUMENT,
    };
    let response = match unsafe { response_mut(response) } {
        Ok(response) => response,
        Err(status) => return status,
    };
    if response.options.len() >= u8::MAX as usize {
        return MMA_INVALID_ARGUMENT;
    }
    response.options.push((key.to_owned(), value.to_owned()));
    MMA_OK
}
