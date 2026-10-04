#![allow(unsafe_op_in_unsafe_fn)]

use crate::core::config::Config as CoreConfig;
use crate::core::router::Router;
use crate::core::server::Server;
use crate::protocol::commands::{Request, Response, ResponseHeaders, ResponseOpcode};
use crate::protocol::config::{FramerConfig, MIN_HEADER_LEN, RESERVED_SUFFIX};
use crate::protocol::framer::Framer;
use bytes::{Bytes, BytesMut};
use std::ffi::CStr;
use std::mem;
use std::net::{IpAddr, SocketAddr};
use std::os::raw::{c_char, c_void};
use std::ptr;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use tokio::sync::oneshot;
use tokio_util::codec::{Decoder, Encoder};

pub const MMA_STATUS_OK: i32 = 0;
pub const MMA_STATUS_INCOMPLETE: i32 = 1;
pub const MMA_STATUS_INVALID_ARGUMENT: i32 = -1;
pub const MMA_STATUS_INVALID_CONFIG: i32 = -2;
pub const MMA_STATUS_ENCODE_ERROR: i32 = -3;
pub const MMA_STATUS_DECODE_ERROR: i32 = -4;
pub const MMA_STATUS_SERVER_ERROR: i32 = -5;
pub const MMA_STATUS_SERVER_RUNNING: i32 = -6;
pub const MMA_STATUS_CALLBACK_ERROR: i32 = -7;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MMAFramerConfig {
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
pub struct MMAOption {
    pub key: *const u8,
    pub key_len: usize,
    pub value: *const u8,
    pub value_len: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MMAResponse {
    pub opcode: u8,
    pub version: u8,
    pub req_id: u32,
    pub payload: *const u8,
    pub payload_len: usize,
    pub options: *const MMAOption,
    pub options_count: usize,
}

#[repr(C)]
pub struct MMABytes {
    pub data: *mut u8,
    pub len: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MMAServerConfig {
    pub bind_ip: *const c_char,
    pub port: u16,
    pub max_batch: u8,
    pub framer: MMAFramerConfig,
}

pub type MMARouteCallback =
    Option<extern "C" fn(*const MMARequest, *mut MMARouteResponse, *mut c_void) -> i32>;

pub struct MMAFramer {
    framer: Framer,
    input: BytesMut,
}

pub struct MMARequest {
    request: Request,
}

pub struct MMARouteResponse {
    response: *mut Response,
}

struct RegisteredRoute {
    route: String,
    callback: extern "C" fn(*const MMARequest, *mut MMARouteResponse, *mut c_void) -> i32,
    user_data: usize,
}

struct RunningServer {
    shutdown: oneshot::Sender<()>,
    thread: JoinHandle<()>,
    bound_port: u16,
}

pub struct MMAServer {
    config: CoreConfig,
    routes: Vec<RegisteredRoute>,
    running: Option<RunningServer>,
}

fn config_from_c(config: MMAFramerConfig) -> FramerConfig {
    FramerConfig {
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
    }
}

fn valid_slice<T>(data: *const T, len: usize) -> bool {
    len == 0 || !data.is_null()
}

unsafe fn raw_slice<'a, T>(data: *const T, len: usize) -> &'a [T] {
    if len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(data, len)
    }
}

fn response_opcode(value: u8) -> Option<ResponseOpcode> {
    match value {
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

fn copy_response(response: &MMAResponse) -> Result<Response, i32> {
    if !valid_slice(response.payload, response.payload_len)
        || !valid_slice(response.options, response.options_count)
    {
        return Err(MMA_STATUS_INVALID_ARGUMENT);
    }
    let payload =
        unsafe { Bytes::copy_from_slice(raw_slice(response.payload, response.payload_len)) };
    let options = unsafe {
        raw_slice(response.options, response.options_count)
            .iter()
            .map(|option| {
                if !valid_slice(option.key, option.key_len)
                    || !valid_slice(option.value, option.value_len)
                {
                    return Err(MMA_STATUS_INVALID_ARGUMENT);
                }
                let key = std::str::from_utf8(raw_slice(option.key, option.key_len))
                    .map_err(|_| MMA_STATUS_INVALID_ARGUMENT)?
                    .to_owned();
                let value = std::str::from_utf8(raw_slice(option.value, option.value_len))
                    .map_err(|_| MMA_STATUS_INVALID_ARGUMENT)?
                    .to_owned();
                Ok((key, value))
            })
            .collect::<Result<Vec<_>, i32>>()?
    };
    let opcode = response_opcode(response.opcode).ok_or(MMA_STATUS_INVALID_ARGUMENT)?;
    Ok(Response {
        headers: ResponseHeaders {
            opcode,
            version: response.version,
            req_id: response.req_id,
        },
        payload,
        options,
    })
}

unsafe fn route_response_mut<'a>(response: *mut MMARouteResponse) -> Result<&'a mut Response, i32> {
    if response.is_null() || (*response).response.is_null() {
        return Err(MMA_STATUS_INVALID_ARGUMENT);
    }
    Ok(&mut *(*response).response)
}

fn write_bytes(bytes: &[u8], output: *mut MMABytes) -> i32 {
    if output.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let owned = bytes.to_vec().into_boxed_slice();
    let result = MMABytes {
        data: if owned.is_empty() {
            ptr::null_mut()
        } else {
            owned.as_ptr() as *mut u8
        },
        len: owned.len(),
    };
    mem::forget(owned);
    unsafe {
        *output = result;
    }
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn mma_framer_config_default(output: *mut MMAFramerConfig) -> i32 {
    if output.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let config = FramerConfig::new();
    unsafe {
        *output = MMAFramerConfig {
            max_message_length: config.max_message_length,
            opcode_pos: config.opcode_pos,
            version_pos: config.version_pos,
            route_len_pos: config.route_len_pos,
            route_order: config.route_order,
            payload_order: config.payload_order,
            options_order: config.options_order,
            options_count_pos: config.options_count_pos,
            options_key_first: config.options_key_first as u8,
            // FramerConfig::validate reserves a 12-byte suffix in addition to
            // the minimum logical header fields.
            header_length: MIN_HEADER_LEN + RESERVED_SUFFIX,
        };
    }
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn mma_framer_create(
    config: *const MMAFramerConfig,
    output: *mut *mut MMAFramer,
) -> i32 {
    if config.is_null() || output.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let config = config_from_c(unsafe { *config });
    if config.validate().is_err() {
        return MMA_STATUS_INVALID_CONFIG;
    }
    let handle = Box::new(MMAFramer {
        framer: Framer::new(config),
        input: BytesMut::new(),
    });
    unsafe {
        *output = Box::into_raw(handle);
    }
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_framer_destroy(framer: *mut MMAFramer) {
    if !framer.is_null() {
        drop(Box::from_raw(framer));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_response_encode(
    framer: *mut MMAFramer,
    response: *const MMAResponse,
    output: *mut MMABytes,
) -> i32 {
    if framer.is_null() || response.is_null() || output.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let response = match copy_response(&*response) {
        Ok(response) => response,
        Err(status) => return status,
    };
    let mut encoded = BytesMut::new();
    match (*framer).framer.encode(response, &mut encoded) {
        Ok(()) => write_bytes(&encoded, output),
        Err(_) => MMA_STATUS_ENCODE_ERROR,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_bytes_free(bytes: MMABytes) {
    if !bytes.data.is_null() {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            bytes.data, bytes.len,
        )));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_framer_decode(
    framer: *mut MMAFramer,
    data: *const u8,
    data_len: usize,
    output: *mut *mut MMARequest,
) -> i32 {
    if framer.is_null() || !valid_slice(data, data_len) || output.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    *output = ptr::null_mut();
    if data_len != 0 {
        (*framer)
            .input
            .extend_from_slice(std::slice::from_raw_parts(data, data_len));
    }
    match (*framer).framer.decode(&mut (*framer).input) {
        Ok(Some(request)) => {
            *output = Box::into_raw(Box::new(MMARequest { request }));
            MMA_STATUS_OK
        }
        Ok(None) => MMA_STATUS_INCOMPLETE,
        Err(_) => MMA_STATUS_DECODE_ERROR,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_request_destroy(request: *mut MMARequest) {
    if !request.is_null() {
        drop(Box::from_raw(request));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_request_payload(
    request: *const MMARequest,
    data: *mut *const u8,
    len: *mut usize,
) -> i32 {
    if request.is_null() || data.is_null() || len.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let request_ref = unsafe { &*request };
    *data = request_ref.request.payload.as_ptr();
    *len = request_ref.request.payload.len();
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_request_route(
    request: *const MMARequest,
    data: *mut *const u8,
    len: *mut usize,
) -> i32 {
    if request.is_null() || data.is_null() || len.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let request_ref = unsafe { &*request };
    *data = request_ref.request.route.as_ptr();
    *len = request_ref.request.route.len();
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_request_metadata(
    request: *const MMARequest,
    opcode: *mut u8,
    version: *mut u8,
    req_id: *mut u32,
) -> i32 {
    if request.is_null() || opcode.is_null() || version.is_null() || req_id.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let request_ref = unsafe { &*request };
    *opcode = request_ref.request.headers.opcode as u8;
    *version = request_ref.request.headers.version;
    *req_id = request_ref.request.headers.req_id;
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_request_options_count(
    request: *const MMARequest,
    count: *mut usize,
) -> i32 {
    if request.is_null() || count.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let request_ref = unsafe { &*request };
    *count = request_ref.request.options.len();
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_request_option(
    request: *const MMARequest,
    index: usize,
    key: *mut *const u8,
    key_len: *mut usize,
    value: *mut *const u8,
    value_len: *mut usize,
) -> i32 {
    if request.is_null()
        || key.is_null()
        || key_len.is_null()
        || value.is_null()
        || value_len.is_null()
    {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let request_ref = unsafe { &*request };
    let Some((option_key, option_value)) = request_ref.request.options.get(index) else {
        return MMA_STATUS_INVALID_ARGUMENT;
    };
    *key = option_key.as_ptr();
    *key_len = option_key.len();
    *value = option_value.as_ptr();
    *value_len = option_value.len();
    MMA_STATUS_OK
}

fn router_from_routes(routes: &[RegisteredRoute]) -> Router {
    let mut router = Router::new();
    for registered in routes {
        let callback = registered.callback;
        let user_data = registered.user_data;
        router.register_route(
            registered.route.clone(),
            Arc::new(move |request, response| {
                let request = Box::new(MMARequest { request });
                let mut route_response = MMARouteResponse {
                    response: response as *mut Response,
                };
                let status = callback(&*request, &mut route_response, user_data as *mut c_void);
                if status != MMA_STATUS_OK {
                    response.headers.opcode = ResponseOpcode::InternalError;
                    response.payload = Bytes::new();
                    response.options.clear();
                }
            }),
        );
    }
    router
}

#[unsafe(no_mangle)]
pub extern "C" fn mma_server_config_default(output: *mut MMAServerConfig) -> i32 {
    if output.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let mut framer = MMAFramerConfig {
        max_message_length: 0,
        opcode_pos: 0,
        version_pos: 0,
        route_len_pos: 0,
        route_order: 0,
        payload_order: 0,
        options_order: 0,
        options_count_pos: 0,
        options_key_first: 0,
        header_length: 0,
    };
    if mma_framer_config_default(&mut framer) != MMA_STATUS_OK {
        return MMA_STATUS_INVALID_CONFIG;
    }
    unsafe {
        *output = MMAServerConfig {
            bind_ip: c"0.0.0.0".as_ptr(),
            port: 8080,
            max_batch: 16,
            framer,
        };
    }
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_server_create(
    config: *const MMAServerConfig,
    output: *mut *mut MMAServer,
) -> i32 {
    if config.is_null() || output.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    *output = ptr::null_mut();
    let config = &*config;
    if config.bind_ip.is_null() || config.framer.options_key_first > 1 {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let Ok(bind_ip) = CStr::from_ptr(config.bind_ip).to_str() else {
        return MMA_STATUS_INVALID_ARGUMENT;
    };
    let Ok(ip) = bind_ip.parse::<IpAddr>() else {
        return MMA_STATUS_INVALID_ARGUMENT;
    };
    let framer = config_from_c(config.framer);
    if framer.validate().is_err() {
        return MMA_STATUS_INVALID_CONFIG;
    }
    let server = MMAServer {
        config: CoreConfig {
            address: SocketAddr::new(ip, config.port),
            frame_config: framer,
            max_batch: config.max_batch,
        },
        routes: Vec::new(),
        running: None,
    };
    *output = Box::into_raw(Box::new(server));
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_server_register_route(
    server: *mut MMAServer,
    route: *const u8,
    route_len: usize,
    callback: MMARouteCallback,
    user_data: *mut c_void,
) -> i32 {
    if server.is_null() || !valid_slice(route, route_len) {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let server = &mut *server;
    if server.running.is_some() {
        return MMA_STATUS_SERVER_RUNNING;
    }
    let Ok(route) = std::str::from_utf8(raw_slice(route, route_len)) else {
        return MMA_STATUS_INVALID_ARGUMENT;
    };
    if route.is_empty() || route_len > u8::MAX as usize {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let Some(callback) = callback else {
        return MMA_STATUS_INVALID_ARGUMENT;
    };
    server.routes.push(RegisteredRoute {
        route: route.to_owned(),
        callback,
        user_data: user_data as usize,
    });
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_server_start(server: *mut MMAServer) -> i32 {
    if server.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let server = &mut *server;
    if server.running.is_some() {
        return MMA_STATUS_SERVER_RUNNING;
    }

    let config = server.config.clone();
    let router = router_from_routes(&server.routes);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let thread = thread::Builder::new()
        .name("mma-c-server".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(_) => {
                    let _ = started_tx.send(Err(MMA_STATUS_SERVER_ERROR));
                    return;
                }
            };
            runtime.block_on(async move {
                let mut server = match Server::new(config).await {
                    Ok(server) => server,
                    Err(_) => {
                        let _ = started_tx.send(Err(MMA_STATUS_SERVER_ERROR));
                        return;
                    }
                };
                let bound_port = match server.local_addr() {
                    Ok(address) => address.port(),
                    Err(_) => {
                        let _ = started_tx.send(Err(MMA_STATUS_SERVER_ERROR));
                        return;
                    }
                };
                if started_tx.send(Ok(bound_port)).is_err() {
                    return;
                }
                tokio::select! {
                    _ = server.run(router) => {}
                    _ = shutdown_rx => {}
                }
            });
        });
    let thread = match thread {
        Ok(thread) => thread,
        Err(_) => return MMA_STATUS_SERVER_ERROR,
    };
    match started_rx.recv() {
        Ok(Ok(bound_port)) => {
            server.running = Some(RunningServer {
                shutdown: shutdown_tx,
                thread,
                bound_port,
            });
            MMA_STATUS_OK
        }
        Ok(Err(status)) => {
            let _ = thread.join();
            status
        }
        Err(_) => {
            let _ = thread.join();
            MMA_STATUS_SERVER_ERROR
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_server_bound_port(server: *const MMAServer, port: *mut u16) -> i32 {
    if server.is_null() || port.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let Some(running) = (&*server).running.as_ref() else {
        return MMA_STATUS_INVALID_ARGUMENT;
    };
    *port = running.bound_port;
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_server_stop(server: *mut MMAServer) -> i32 {
    if server.is_null() {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let server = &mut *server;
    let Some(running) = server.running.take() else {
        return MMA_STATUS_OK;
    };
    let _ = running.shutdown.send(());
    if running.thread.join().is_err() {
        return MMA_STATUS_SERVER_ERROR;
    }
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_server_destroy(server: *mut MMAServer) {
    if !server.is_null() {
        let _ = mma_server_stop(server);
        drop(Box::from_raw(server));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_response_builder_set_status(
    response: *mut MMARouteResponse,
    opcode: u8,
    version: u8,
) -> i32 {
    mma_response_set_status(response, opcode, version)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_response_set_status(
    response: *mut MMARouteResponse,
    opcode: u8,
    version: u8,
) -> i32 {
    let Some(opcode) = response_opcode(opcode) else {
        return MMA_STATUS_INVALID_ARGUMENT;
    };
    let response = match route_response_mut(response) {
        Ok(response) => response,
        Err(status) => return status,
    };
    response.headers.opcode = opcode;
    response.headers.version = version;
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_response_builder_set_payload(
    response: *mut MMARouteResponse,
    data: *const u8,
    data_len: usize,
) -> i32 {
    mma_response_set_payload(response, data, data_len)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_response_set_payload(
    response: *mut MMARouteResponse,
    data: *const u8,
    data_len: usize,
) -> i32 {
    if !valid_slice(data, data_len) {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let response = match route_response_mut(response) {
        Ok(response) => response,
        Err(status) => return status,
    };
    response.payload = Bytes::copy_from_slice(raw_slice(data, data_len));
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_response_builder_add_option(
    response: *mut MMARouteResponse,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    mma_response_add_option(response, key, key_len, value, value_len)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn mma_response_add_option(
    response: *mut MMARouteResponse,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    if !valid_slice(key, key_len)
        || !valid_slice(value, value_len)
        || key_len == 0
        || value_len == 0
    {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let Ok(key) = std::str::from_utf8(raw_slice(key, key_len)) else {
        return MMA_STATUS_INVALID_ARGUMENT;
    };
    let Ok(value) = std::str::from_utf8(raw_slice(value, value_len)) else {
        return MMA_STATUS_INVALID_ARGUMENT;
    };
    if key_len > u16::MAX as usize || value_len > u16::MAX as usize {
        return MMA_STATUS_INVALID_ARGUMENT;
    }
    let response = match route_response_mut(response) {
        Ok(response) => response,
        Err(status) => return status,
    };
    response.options.push((key.to_owned(), value.to_owned()));
    MMA_STATUS_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn mma_status_message(status: i32) -> *const std::os::raw::c_char {
    match status {
        MMA_STATUS_OK => b"success\0".as_ptr() as *const _,
        MMA_STATUS_INCOMPLETE => b"more bytes are required\0".as_ptr() as *const _,
        MMA_STATUS_INVALID_ARGUMENT => b"invalid argument\0".as_ptr() as *const _,
        MMA_STATUS_INVALID_CONFIG => b"invalid framer configuration\0".as_ptr() as *const _,
        MMA_STATUS_ENCODE_ERROR => b"frame encoding failed\0".as_ptr() as *const _,
        MMA_STATUS_DECODE_ERROR => b"frame decoding failed\0".as_ptr() as *const _,
        MMA_STATUS_SERVER_ERROR => b"server operation failed\0".as_ptr() as *const _,
        MMA_STATUS_SERVER_RUNNING => b"server is already running\0".as_ptr() as *const _,
        MMA_STATUS_CALLBACK_ERROR => b"route callback failed\0".as_ptr() as *const _,
        _ => b"unknown status\0".as_ptr() as *const _,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::commands::{RequestHeaders, RequestOpcode};

    extern "C" fn echo_callback(
        request: *const MMARequest,
        response: *mut MMARouteResponse,
        _user_data: *mut c_void,
    ) -> i32 {
        let mut payload = ptr::null();
        let mut payload_len = 0;
        let status = unsafe { mma_request_payload(request, &mut payload, &mut payload_len) };
        if status != MMA_STATUS_OK {
            return status;
        }
        let status = unsafe { mma_response_set_status(response, ResponseOpcode::Message as u8, 1) };
        if status != MMA_STATUS_OK {
            return status;
        }
        let status = unsafe {
            mma_response_add_option(
                response,
                c"type".as_ptr().cast(),
                4,
                c"echo".as_ptr().cast(),
                4,
            )
        };
        if status != MMA_STATUS_OK {
            return status;
        }
        unsafe { mma_response_set_payload(response, payload, payload_len) }
    }

    extern "C" fn failing_callback(
        _request: *const MMARequest,
        response: *mut MMARouteResponse,
        _user_data: *mut c_void,
    ) -> i32 {
        let payload = b"will be cleared";
        let _ = unsafe { mma_response_set_payload(response, payload.as_ptr(), payload.len()) };
        MMA_STATUS_CALLBACK_ERROR
    }

    fn request(route: &str, payload: &'static [u8], req_id: u32) -> Request {
        Request {
            headers: RequestHeaders {
                opcode: RequestOpcode::Once,
                version: 1,
                payload_len: payload.len() as u32,
                route_len: route.len() as u8,
                options_len: 0,
                options_count: 0,
                req_id,
            },
            payload: Bytes::from_static(payload),
            route: route.to_owned(),
            options: Vec::new(),
        }
    }

    #[test]
    fn encode_and_decode_incremental_frame() {
        let mut config = MMAFramerConfig {
            max_message_length: 0,
            opcode_pos: 0,
            version_pos: 0,
            route_len_pos: 0,
            route_order: 0,
            payload_order: 0,
            options_order: 0,
            options_count_pos: 0,
            options_key_first: 0,
            header_length: 0,
        };
        assert_eq!(mma_framer_config_default(&mut config), MMA_STATUS_OK);

        let mut framer = ptr::null_mut();
        assert_eq!(mma_framer_create(&config, &mut framer), MMA_STATUS_OK);

        let payload = b"hello";
        let response = MMAResponse {
            opcode: ResponseOpcode::Ok as u8,
            version: 1,
            req_id: 42,
            payload: payload.as_ptr(),
            payload_len: payload.len(),
            options: ptr::null(),
            options_count: 0,
        };
        let mut encoded = MMABytes {
            data: ptr::null_mut(),
            len: 0,
        };
        assert_eq!(
            unsafe { mma_response_encode(framer, &response, &mut encoded) },
            MMA_STATUS_OK
        );

        let mut request = ptr::null_mut();
        assert_eq!(
            unsafe { mma_framer_decode(framer, encoded.data, 3, &mut request) },
            MMA_STATUS_INCOMPLETE
        );
        assert!(request.is_null());
        assert_eq!(
            unsafe {
                mma_framer_decode(framer, encoded.data.add(3), encoded.len - 3, &mut request)
            },
            MMA_STATUS_OK
        );

        let mut decoded_payload = ptr::null();
        let mut decoded_len = 0;
        assert_eq!(
            unsafe { mma_request_payload(request, &mut decoded_payload, &mut decoded_len) },
            MMA_STATUS_OK
        );
        assert_eq!(
            unsafe { std::slice::from_raw_parts(decoded_payload, decoded_len) },
            payload
        );

        unsafe {
            mma_request_destroy(request);
            mma_bytes_free(encoded);
            mma_framer_destroy(framer);
        }
    }

    #[test]
    fn route_callback_mutates_live_response() {
        let routes = vec![RegisteredRoute {
            route: "ECHO".to_owned(),
            callback: echo_callback,
            user_data: 0,
        }];
        let router = router_from_routes(&routes);

        let response = router.handle(request("ECHO", b"hello", 77));

        assert_eq!(response.headers.opcode, ResponseOpcode::Message);
        assert_eq!(response.headers.version, 1);
        assert_eq!(response.headers.req_id, 77);
        assert_eq!(&response.payload[..], b"hello");
        assert_eq!(
            response.options,
            vec![("type".to_owned(), "echo".to_owned())]
        );
    }

    #[test]
    fn route_callback_error_marks_internal_error() {
        let routes = vec![RegisteredRoute {
            route: "FAIL".to_owned(),
            callback: failing_callback,
            user_data: 0,
        }];
        let router = router_from_routes(&routes);

        let response = router.handle(request("FAIL", b"payload", 88));

        assert_eq!(response.headers.opcode, ResponseOpcode::InternalError);
        assert_eq!(response.headers.version, 1);
        assert_eq!(response.headers.req_id, 88);
        assert!(response.payload.is_empty());
        assert!(response.options.is_empty());
    }
}
