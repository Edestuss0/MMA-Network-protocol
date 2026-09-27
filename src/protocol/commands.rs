use bytes::Bytes;
use std::collections::HashMap;
use std::io;
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
    pub req_id: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum RequestOpcode {
    Once = 2,
    Channel = 1,
}

impl TryFrom<u8> for RequestOpcode {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(RequestOpcode::Once),
            1 => Ok(RequestOpcode::Channel),
            _ => Err(()),
        }
    }
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
    pub req_id: u32,
}

#[derive(Debug)]
pub struct OptionValue {
    pub key: Bytes,
    pub value: Bytes,
}

pub const RESPONSE_OPCODES_COUNT: usize = 8;

#[derive(Debug, Clone, Eq, Hash, PartialEq, Copy)]
pub enum ResponseOpcode {
    Ok = 1,
    BadRequest = 2,
    Unauthorized = 3,
    Forbidden = 4,
    NotFound = 5,
    Conflict = 6,
    InternalError = 7,
    Message = 8,
}

#[derive(Debug)]
pub struct Response {
    pub headers: ResponseHeaders,
    pub payload: Bytes,
    pub options: Vec<(String, String)>,
}
