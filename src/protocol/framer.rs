use std::sync::{Arc, RwLock};

pub(crate) use crate::core::commands::{
    ProtocolError, Request, RequestHeaders, RequestOpcode, Response, ResponseHeaders,
};
use crate::core::config::{self, BigBytesConfig, FramerConfig, RESERVED_SUFFIX};
use ahash::AHashSet;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, Debug)]
pub struct Framer {
    pub config: Arc<RwLock<FramerConfig>>,
    cached_header: Option<RequestHeaders>,
    cached_config: Option<FramerConfig>,
}

impl Framer {
    pub fn new(cfg: Arc<RwLock<FramerConfig>>) -> Self {
        Self {
            config: cfg,
            cached_header: None,
            cached_config: None,
        }
    }
}

impl Encoder<Response> for Framer {
    type Error = ProtocolError;

    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.options.len() > u8::MAX as usize {
            return Err(ProtocolError::InvalidFrame {
                message: "Too many options",
                req_id: None,
            });
        }

        let config = self
            .config
            .read()
            .expect("Framer config lock poisoned")
            .clone();

        let hlen = config.header_length as usize;
        let frame_start = dst.len();
        dst.resize(frame_start + hlen, 0);

        let option_length;

        if config.payload_order > config.options_order {
            option_length =
                match encode_options(&item.options, &config, dst, Some(item.headers.req_id)) {
                    Ok(len) => len,
                    Err(e) => {
                        dst.truncate(frame_start);
                        return Err(e);
                    }
                };
            dst.put_slice(&item.payload);
        } else {
            dst.put_slice(&item.payload);

            option_length =
                match encode_options(&item.options, &config, dst, Some(item.headers.req_id)) {
                    Ok(len) => len,
                    Err(e) => {
                        dst.truncate(frame_start);
                        return Err(e);
                    }
                };
        }

        let body_length = item
            .payload
            .len()
            .checked_add(option_length as usize)
            .ok_or(ProtocolError::InvalidFrame {
                message: "Frame length overflow",
                req_id: Some(item.headers.req_id),
            })?;

        if body_length > config.max_message_length as usize {
            dst.truncate(frame_start);
            return Err(ProtocolError::MessageTooLarge {
                message: body_length,
                req_id: None,
            });
        }

        match encode_headers(
            &item.headers,
            item.options.len() as u8,
            option_length,
            item.payload.len() as u32,
            &config,
            dst,
            frame_start,
        ) {
            Ok(()) => {}
            Err(e) => {
                return {
                    dst.truncate(frame_start);
                    Err(e)
                };
            }
        };

        Ok(())
    }
}

impl Decoder for Framer {
    type Item = Request;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let config = match &self.cached_config {
            Some(config) => config.clone(),
            None => self
                .config
                .read()
                .expect("Framer config lock poisoned")
                .clone(),
        };

        if src.len() < config.header_length as usize {
            self.cached_config = Some(config);
            return Ok(None);
        }

        let headers: RequestHeaders = match &self.cached_header {
            Some(val) => val.clone(),
            None => match decode_headers(&src[..config.header_length as usize], &config) {
                Ok(value) => value,
                Err(e) => {
                    return Err(e);
                }
            },
        };

        let body_len = (headers.payload_len as usize)
            .checked_add(headers.route_len as usize)
            .and_then(|v| v.checked_add(headers.options_len as usize))
            .ok_or(ProtocolError::InvalidFrame {
                message: "Frame length overflow",
                req_id: Some(headers.req_id),
            })?;

        if body_len > config.max_message_length as usize {
            return Err(ProtocolError::MessageTooLarge {
                message: body_len,
                req_id: Some(headers.req_id),
            });
        }

        let frame_len = (config.header_length as usize)
            .checked_add(body_len)
            .ok_or(ProtocolError::InvalidFrame {
                message: "Frame length overflow",
                req_id: Some(headers.req_id),
            })?;

        if src.len() < frame_len {
            self.cached_config = Some(config);
            self.cached_header = Some(headers);
            return Ok(None);
        } else {
            self.cached_header = None;
            self.cached_config = None;
        }

        let _hdr = src.split_to(config.header_length as usize);

        let mut payload = Bytes::new();
        let mut route = String::new();
        let mut options: Vec<(Bytes, Bytes)> = Vec::with_capacity(headers.options_count as usize);

        for i in 1..=3 {
            if config.options_order == i {
                let option_bytes = src.split_to(headers.options_len as usize);
                options = decode_options(option_bytes, &config, &headers, Some(headers.req_id))?;
            } else if config.route_order == i {
                let route_bytes = src.split_to(headers.route_len as usize);
                route = String::from_utf8(route_bytes.into()).map_err(|_| {
                    ProtocolError::InvalidFrame {
                        message: "Invalid route",
                        req_id: Some(headers.req_id),
                    }
                })?;
            } else if config.payload_order == i {
                payload = src.split_to(headers.payload_len as usize).freeze();
            }
        }

        Ok(Some(Request {
            payload,
            headers,
            route,
            options,
        }))
    }
}

const MIN_OPTION_LENGTH: usize = 2 + 2 + 1 + 1;

fn encode_options(
    options: &Vec<(Bytes, Bytes)>,
    config: &FramerConfig,
    dst: &mut BytesMut,
    req_id: Option<u32>,
) -> Result<u32, ProtocolError> {
    let start = dst.len();

    let mut validate_set = AHashSet::<&[u8]>::with_capacity(options.len());

    for (key, value) in options {
        if key.is_empty() {
            return Err(ProtocolError::InvalidFrame {
                message: "Option can't be empty",
                req_id,
            });
        }

        if key.len() > u16::MAX as usize || value.len() > u16::MAX as usize {
            return Err(ProtocolError::InvalidFrame {
                message: "Option too large",
                req_id,
            });
        }

        if !validate_set.insert(key) {
            return Err(ProtocolError::InvalidFrame {
                message: "Duplicate option key",
                req_id,
            });
        }

        dst.put_u16(key.len() as u16);
        dst.put_u16(value.len() as u16);

        if config.options_key_first {
            dst.put_slice(key);
            dst.put_slice(value);
        } else {
            dst.put_slice(value);
            dst.put_slice(key);
        }
    }

    Ok((dst.len() - start) as u32)
}

fn decode_options(
    raw: BytesMut,
    config: &FramerConfig,
    headers: &RequestHeaders,
    req_id: Option<u32>,
) -> Result<Vec<(Bytes, Bytes)>, ProtocolError> {
    let count = headers.options_count as usize;

    if count == 0 {
        if !raw.is_empty() {
            return Err(ProtocolError::InvalidFrame {
                message: "Invalid options format",
                req_id,
            });
        }

        return Ok(Vec::new());
    }

    let mut raw = raw.freeze();
    let mut options = Vec::with_capacity(count);
    let mut validate_set = AHashSet::<Bytes>::with_capacity(count);

    for _ in 0..count {
        if raw.len() < MIN_OPTION_LENGTH {
            return Err(ProtocolError::InvalidFrame {
                message: "Invalid options format",
                req_id,
            });
        }

        let key_len = raw.get_u16() as usize;
        let value_len = raw.get_u16() as usize;

        let data_len = key_len
            .checked_add(value_len)
            .ok_or(ProtocolError::InvalidFrame {
                message: "Option length overflow",
                req_id,
            })?;

        if key_len == 0 || value_len == 0 || raw.len() < data_len {
            return Err(ProtocolError::InvalidFrame {
                message: "Invalid options format",
                req_id,
            });
        }

        let (key, value) = if config.options_key_first {
            let key = raw.split_to(key_len);
            let value = raw.split_to(value_len);
            (key, value)
        } else {
            let value = raw.split_to(value_len);
            let key = raw.split_to(key_len);
            (key, value)
        };

        if !validate_set.insert(key.clone()) {
            return Err(ProtocolError::InvalidFrame {
                message: "Duplicate option key",
                req_id,
            });
        };

        options.push((key, value));
    }

    if !raw.is_empty() {
        return Err(ProtocolError::InvalidFrame {
            message: "Trailing bytes in options",
            req_id,
        });
    }

    Ok(options)
}

fn encode_headers(
    headers: &ResponseHeaders,
    options_count: u8,
    options_len: u32,
    payload_len: u32,
    config: &FramerConfig,
    dst: &mut BytesMut,
    frame_start: usize,
) -> Result<(), ProtocolError> {
    let hlen = config.header_length as usize;

    let frame_end = frame_start
        .checked_add(hlen)
        .ok_or(ProtocolError::InvalidFrame {
            message: "Header position overflow",
            req_id: None,
        })?;

    if dst.len() < frame_end {
        return Err(ProtocolError::InvalidFrame {
            message: "Header buffer is too short",
            req_id: None,
        });
    }

    let opcode = headers.opcode as u8;

    dst[frame_start + (config.version_pos - 1) as usize] = headers.version;

    dst[frame_start + (config.opcode_pos - 1) as usize] = opcode;

    dst[frame_start + (config.options_count_pos - 1) as usize] = options_count;

    encode_big_bytes(dst, frame_start, config.header_length, &config.big_bytes_config, (options_len, payload_len, headers.req_id));

    Ok(())
}

fn encode_big_bytes(
    dst: &mut BytesMut,
    frame_start: usize,
    header_length: u8,
    config: &BigBytesConfig,
    vals: (u32, u32, u32),
) {
    let (options_len, payload_len, req_id) = vals;

    let big_bytes_start = frame_start + header_length as usize - RESERVED_SUFFIX as usize;

    let opt_pos = big_bytes_start + (config.options_len_order as usize - 1) * 4;
    let pay_pos = big_bytes_start + (config.payload_len_order as usize - 1) * 4;
    let req_pos = big_bytes_start + (config.req_id_order as usize - 1) * 4;

    dst[opt_pos..opt_pos + 4].copy_from_slice(&options_len.to_be_bytes());
    dst[pay_pos..pay_pos + 4].copy_from_slice(&payload_len.to_be_bytes());
    dst[req_pos..req_pos + 4].copy_from_slice(&req_id.to_be_bytes());
}

fn decode_big_bytes(bytes: &[u8; 12], config: &BigBytesConfig) -> Result<(u32, u32, u32), ()> {
    let val1 = u32::from_be_bytes(bytes[0..4].try_into().map_err(|_| ())?);
    let val2 = u32::from_be_bytes(bytes[4..8].try_into().map_err(|_| ())?);
    let val3 = u32::from_be_bytes(bytes[8..12].try_into().map_err(|_| ())?);

    let vals = [val1, val2, val3];

    let payload_len = vals[(config.payload_len_order - 1) as usize];
    let options_len = vals[(config.options_len_order - 1) as usize];
    let req_id = vals[(config.req_id_order - 1) as usize];

    Ok((payload_len, options_len, req_id))
}

fn decode_headers(bytes: &[u8], config: &FramerConfig) -> Result<RequestHeaders, ProtocolError> {
    if bytes.len() < config.header_length as usize {
        return Err(ProtocolError::InvalidFrame {
            message: "Headers slice is too short",
            req_id: None,
        });
    }

    let big_bytes: &[u8; 12] = (&bytes[bytes.len() - 12..]).try_into().map_err(|_| ProtocolError::InvalidFrame {message: "Invalid big bytes trail", req_id: None})?;

    let (payload_len, options_len, req_id) = decode_big_bytes(big_bytes, &config.big_bytes_config).map_err(|_| ProtocolError::InvalidFrame {message: "Invalid big bytes trail", req_id: None})?;

    let opcode: RequestOpcode = match bytes.get((config.opcode_pos - 1) as usize) {
        Some(&value) => match RequestOpcode::try_from(value) {
            Ok(value) => value,
            Err(_) => {
                return Err(ProtocolError::InvalidFrame {
                    message: "Invalid opcode",
                    req_id: Some(req_id),
                });
            }
        },
        None => {
            return Err(ProtocolError::InvalidFrame {
                message: "Opcode not found",
                req_id: Some(req_id),
            });
        }
    };

    let version: u8 = match bytes.get((config.version_pos - 1) as usize) {
        Some(value) => *value,
        None => {
            return Err(ProtocolError::InvalidFrame {
                message: "Version not found",
                req_id: Some(req_id),
            });
        }
    };

    let route_bytes = bytes.get((config.route_len_pos - 1) as usize);
    let route_len: u8 = match route_bytes {
        Some(value) => *value,
        None => {
            return Err(ProtocolError::InvalidFrame {
                message: "Route length not found",
                req_id: Some(req_id),
            });
        }
    };

    let options_count_bytes = bytes.get((config.options_count_pos - 1) as usize);
    let options_count: u8 = match options_count_bytes {
        Some(value) => *value,
        None => {
            return Err(ProtocolError::InvalidFrame {
                message: "Route length not found",
                req_id: Some(req_id),
            });
        }
    };

    Ok(RequestHeaders {
        payload_len,
        options_count,
        version,
        opcode,
        route_len,
        req_id,
        options_len: options_len,
    })
}
