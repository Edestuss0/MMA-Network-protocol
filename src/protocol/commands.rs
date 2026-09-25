use std::collections::HashMap;
use std::io;
use bytes::Bytes;
use thiserror::Error;

pub struct Request {
    pub headers: RequestHeaders,
    pub payload: Bytes,
    pub route: String,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct RequestHeaders {
    pub opcode: RequestOpcode,
    pub version: u8,
    pub payload_len: u32,
    pub route_len: u8,
    pub options_len: u32,
    pub options_count: u8,
}

#[derive(Debug, Clone)]
pub enum RequestOpcode {
    Once,
    Channel,
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),
    #[error("Invalid UTF-8 in key")]
    InvalidUtf8,
    #[error("Invalid frame: {0}")]
    InvalidFrame(&'static str),
    #[error("Message too large: {0} bytes")]
    MessageTooLarge(usize),
}

#[derive(Debug, Clone)]
pub struct ResponseHeaders {
    pub opcode: ResponseOpcode,
    pub version: u8,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum ResponseOpcode {
    Ok,
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    InternalError,
    Message,
}

#[derive(Debug)]
pub struct Response {
    pub headers: ResponseHeaders,
    pub payload: Bytes,
    pub options: Vec<(String, String)>,
}