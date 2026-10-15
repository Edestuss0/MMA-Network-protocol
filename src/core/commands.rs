use crate::core::{
    commands::ResponseOpcode::{
        BadRequest, ChannelConnected, ChannelMessage, Conflict, Forbidden, InternalError, NotFound,
        Ok, Shutdown, Unauthorized,
    },
    config::MMA_VERSION,
};
use bytes::Bytes;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOpcode {
    Once = 2,
    ChannelConnect = 1,
    ChannelMessage = 3,
}

impl TryFrom<u8> for RequestOpcode {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            2 => Result::Ok(RequestOpcode::Once),
            1 => Result::Ok(RequestOpcode::ChannelConnect),
            3 => Result::Ok(RequestOpcode::ChannelMessage),
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
    ChannelMessage = 8,
    Shutdown = 9,
    ChannelConnected = 10,
}

impl ResponseOpcode {
    pub fn from_number(number: u8) -> Option<Self> {
        match number {
            1 => Some(Ok),
            2 => Some(BadRequest),
            3 => Some(Unauthorized),
            4 => Some(Forbidden),
            5 => Some(NotFound),
            6 => Some(Conflict),
            7 => Some(InternalError),
            8 => Some(ChannelMessage),
            9 => Some(Shutdown),
            10 => Some(ChannelConnected),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    pub headers: ResponseHeaders,
    pub payload: Bytes,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct ChannelSendedMessage {
    pub message: Response,
    pub conn_id: u32
}

impl Response {
    pub fn new(req_id: u32) -> Self {
        Self {
            headers: ResponseHeaders {
                version: MMA_VERSION,
                req_id,
                opcode: ResponseOpcode::Ok,
            },
            payload: Bytes::from("Empty response"),
            options: vec![],
        }
    }

    pub fn graceful_shutdown_response(req_id: u32) -> Self {
        Self {
            headers: ResponseHeaders {
                version: MMA_VERSION,
                req_id,
                opcode: ResponseOpcode::Shutdown,
            },
            payload: Bytes::from("Server has been closed"),
            options: vec![],
        }
    }

    pub fn internal_error(req_id: u32) -> Self {
        Self {
            headers: ResponseHeaders {
                version: MMA_VERSION,
                req_id,
                opcode: ResponseOpcode::InternalError,
            },
            payload: Bytes::from("Internal server error"),
            options: vec![],
        }
    }

    pub fn bad_request(req_id: u32, payload: Option<Bytes>) -> Self {
        Self {
            headers: ResponseHeaders {
                version: MMA_VERSION,
                req_id,
                opcode: ResponseOpcode::BadRequest,
            },
            payload: match payload {
                Some(p) => p,
                None => Bytes::from("Bad request"),
            },
            options: vec![],
        }
    }

    pub fn connection_type_mismatch(req_id: u32) -> Self {
        Self {
            headers: ResponseHeaders {
                version: MMA_VERSION,
                req_id,
                opcode: ResponseOpcode::BadRequest,
            },
            payload: Bytes::from("Connection type mismatch"),
            options: vec![],
        }
    }

    pub fn channel_connected(req_id: u32) -> Self {
         Self {
            headers: ResponseHeaders {
                version: MMA_VERSION,
                req_id,
                opcode: ResponseOpcode::ChannelConnected,
            },
            payload: Bytes::from("Channel connected successfully"),
            options: vec![],
        }
    }
}
