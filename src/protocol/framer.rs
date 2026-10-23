pub(crate) use crate::core::commands::{
    ProtocolError, Request, RequestHeaders, RequestOpcode, Response, ResponseHeaders,
};
use crate::core::config::FramerConfig;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, Debug)]
pub struct Framer {
    pub config: FramerConfig,
    cached_header: Option<RequestHeaders>,
}

impl Framer {
    pub fn new(cfg: FramerConfig) -> Self {
        Self {
            config: cfg,
            cached_header: None,
        }
    }
}

impl Encoder<Response> for Framer {
    type Error = ProtocolError;

    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.options.len() > u8::MAX as usize {
            return Err(ProtocolError::InvalidFrame{message: "Too many options", req_id: None});
        }

        let hlen = self.config.header_length as usize;

        let frame_start = dst.len();

        dst.resize(frame_start + hlen, 0);

        let option_length;

        if self.config.payload_order > self.config.options_order {
            option_length = match encode_options(&item.options, &self.config, dst) {
                Ok(len) => len,
                Err(e) => {
                    dst.truncate(frame_start);
                    return Err(e);
                }
            };
            dst.put_slice(&item.payload);
        } else {
            dst.put_slice(&item.payload);

            option_length = match encode_options(&item.options, &self.config, dst) {
                Ok(len) => len,
                Err(e) => {
                    dst.truncate(frame_start);
                    return Err(e);
                }
            };
        }

        let body_length = item.payload.len() as u32 + option_length;

        if body_length > self.config.max_message_length {
            dst.truncate(frame_start);
            return Err(ProtocolError::MessageTooLarge{message: body_length as usize, req_id: None});
        }

        match encode_headers(
            &item.headers,
            item.options.len() as u8,
            option_length,
            item.payload.len() as u32,
            &self.config,
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
        if src.len() < self.config.header_length as usize {
            return Ok(None);
        }

        let headers: RequestHeaders = match &self.cached_header {
            Some(val) => val.clone(),
            None => {
                match decode_headers(&src[..self.config.header_length as usize], &self.config) {
                    Ok(value) => value,
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        };

        let body_len = (headers.payload_len as usize)
            .checked_add(headers.route_len as usize)
            .and_then(|v| v.checked_add(headers.options_len as usize))
            .ok_or(ProtocolError::InvalidFrame{message: "Frame length overflow", req_id: Some(headers.req_id)})?;
        if body_len > self.config.max_message_length as usize {
            return Err(ProtocolError::MessageTooLarge{message: body_len, req_id: Some(headers.req_id)});
        }

        let frame_len = (self.config.header_length as usize)
            .checked_add(body_len)
            .ok_or(ProtocolError::InvalidFrame{message: "Frame length overflow", req_id: Some(headers.req_id)})?;

        if src.len() < frame_len {
            self.cached_header = Some(headers);
            return Ok(None);
        } else {
            self.cached_header = None;
        }

        let _hdr = src.split_to(self.config.header_length as usize);

        let mut payload = Bytes::new();
        let mut route = String::new();
        let mut options: Vec<(Bytes, Bytes)> = Vec::with_capacity(headers.options_count as usize);

        for i in 1..=3 {
            if self.config.options_order == i {
                let option_bytes = src.split_to(headers.options_len as usize);
                options = decode_options(option_bytes, &self.config, &headers)?;
            } else if self.config.route_order == i {
                let route_bytes = src.split_to(headers.route_len as usize);
                route = String::from_utf8(route_bytes.into())
                    .map_err(|_| ProtocolError::InvalidFrame{message: "Invalid route", req_id: Some(headers.req_id)})?;
            } else if self.config.payload_order == i {
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
) -> Result<u32, ProtocolError> {
    let start = dst.len();

    for (key, value) in options {
        if key.is_empty() || value.is_empty() {
            return Err(ProtocolError::InvalidFrame{message: "Option can be empty", req_id: None});
        }

        if key.len() > u16::MAX as usize || value.len() > u16::MAX as usize {
            return Err(ProtocolError::InvalidFrame{message: "Option too large", req_id: None});
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
) -> Result<Vec<(Bytes, Bytes)>, ProtocolError> {
    let count = headers.options_count as usize;

    if count == 0 {
        if !raw.is_empty() {
            return Err(ProtocolError::InvalidFrame{message: "Invalid options format", req_id: None});
        }

        return Ok(Vec::new());
    }

    let mut raw = raw.freeze();
    let mut options = Vec::with_capacity(count);

    for _ in 0..count {
        if raw.len() < 4 {
            return Err(ProtocolError::InvalidFrame{message: "Invalid options format", req_id: None});
        }

        let key_len = raw.get_u16() as usize;
        let value_len = raw.get_u16() as usize;

        let data_len = key_len
            .checked_add(value_len)
            .ok_or(ProtocolError::InvalidFrame{message: "Option length overflow", req_id: None})?;

        if key_len == 0 || value_len == 0 || raw.len() < data_len {
            return Err(ProtocolError::InvalidFrame{message: "Invalid options format", req_id: None});
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

        options.push((key, value));
    }

    if !raw.is_empty() {
        return Err(ProtocolError::InvalidFrame{message: "Trailing bytes in options", req_id: None});
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
        .ok_or(ProtocolError::InvalidFrame{message: "Header position overflow", req_id: None})?;

    if dst.len() < frame_end {
        return Err(ProtocolError::InvalidFrame{message: "Header buffer is too short", req_id: None});
    }

    let opcode = headers.opcode as u8;

    dst[frame_start + (config.version_pos - 1) as usize] = headers.version;

    dst[frame_start + (config.opcode_pos - 1) as usize] = opcode;

    dst[frame_start + (config.options_count_pos - 1) as usize] = options_count;

    let options_start = frame_end - 12;
    let payload_start = frame_end - 8;
    let req_id_start = frame_end - 4;

    dst[options_start..payload_start].copy_from_slice(&options_len.to_be_bytes());
    dst[payload_start..req_id_start].copy_from_slice(&payload_len.to_be_bytes());
    dst[req_id_start..frame_end].copy_from_slice(&headers.req_id.to_be_bytes());

    Ok(())
}

fn decode_headers(bytes: &[u8], config: &FramerConfig) -> Result<RequestHeaders, ProtocolError> {
    if bytes.len() < config.header_length as usize {
        return Err(ProtocolError::InvalidFrame{message: "Headers slice is too short", req_id: None});
    }
    let mut big_bytes = &bytes[bytes.len() - 12..];

    let option_len = big_bytes.get_u32();
    let payload_len = big_bytes.get_u32();
    let req_id = big_bytes.get_u32();
    
    let opcode: RequestOpcode = match bytes.get((config.opcode_pos - 1) as usize) {
        Some(&value) => match RequestOpcode::try_from(value) {
            Ok(value) => value,
            Err(_) => return Err(ProtocolError::InvalidFrame{message: "Invalid opcode", req_id: Some(req_id)}),
        },
        None => {
            return Err(ProtocolError::InvalidFrame{message: "Opcode not found", req_id: Some(req_id)});
        }
    };

    let version: u8 = match bytes.get((config.version_pos - 1) as usize) {
        Some(value) => *value,
        None => {
            return Err(ProtocolError::InvalidFrame{message: "Version not found", req_id: Some(req_id)});
        }
    };

    let route_bytes = bytes.get((config.route_len_pos - 1) as usize);
    let route_len: u8 = match route_bytes {
        Some(value) => *value,
        None => {
            return Err(ProtocolError::InvalidFrame{message: "Route length not found", req_id: Some(req_id)});
        }
    };

    let options_count_bytes = bytes.get((config.options_count_pos - 1) as usize);
    let options_count: u8 = match options_count_bytes {
        Some(value) => *value,
        None => {
            return Err(ProtocolError::InvalidFrame{message:"Route length not found", req_id: Some(req_id)});
        }
    };


    Ok(RequestHeaders {
        payload_len,
        options_count,
        version,
        opcode,
        route_len,
        req_id,
        options_len: option_len,
    })
}
