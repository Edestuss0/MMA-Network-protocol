pub(crate) use crate::protocol::commands::{ProtocolError, Request, RequestHeaders, RequestOpcode, Response, ResponseHeaders, ResponseOpcode};
use crate::protocol::config::FramerConfig;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, Debug)]
pub struct Framer {
    pub config: FramerConfig,
    cached_header: Option<RequestHeaders>,
}

impl Framer {
    pub fn new(cfg: FramerConfig) -> Self {
        Self{
            config: cfg,
            cached_header: None,
        }
    }
}

impl Encoder<Response> for Framer {
    type Error = ProtocolError;

    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {

        if item.options.len() > u8::MAX as usize {
            return Err(ProtocolError::InvalidFrame("Too many options"));
        }


        dst.put_slice(&([0 as u8; 13]));

        if item.payload.len() + item.options.len() > self.config.max_message_length as usize {
            return Err(ProtocolError::MessageTooLarge(item.payload.len() + item.options.len()));
        }
        let option_length: u32 = match calculate_options_length(&item.options) {
            Ok(l) => l,
            Err(e) => return Err(e),
        };
        match encode_headers(&item.headers, item.options.len() as u8, option_length as u32, item.payload.len() as u32,  &self.config, dst) {
            Ok(()) => {},
            Err(e) => {
                return Err(e);
            }
        };

        if self.config.payload_order > self.config.options_order {
            encode_options(&item.options, &self.config, dst);
            dst.put_slice(&item.payload);
        } else {
            dst.put_slice(&item.payload);
            encode_options(&item.options, &self.config, dst)
        }


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

        let mut headers: RequestHeaders;

        headers = match &self.cached_header {
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
            .and_then(|v| {
                v.checked_add(headers.options_len as usize)
            })
            .ok_or(ProtocolError::InvalidFrame(
                "Frame length overflow",
            ))?;
        if body_len > self.config.max_message_length as usize {
            return Err(ProtocolError::MessageTooLarge(body_len));
        }

        let frame_len = (self.config.header_length as usize).checked_add(body_len).ok_or(ProtocolError::InvalidFrame("Frame length overflow"))?;

        if src.len() < frame_len {
            self.cached_header = Some(headers);
            return Ok(None);
        } else {
            self.cached_header = None;
        }

        let _hdr = src.split_to(self.config.header_length as usize);

        let mut payload = Bytes::new();
        let mut route = String::new();
        let mut options: Vec<(String, String)> = Vec::with_capacity(headers.options_count as usize);

        for i in 1..=3 {
            if self.config.options_order == i {
                let option_bytes = src.split_to(headers.options_len as usize);
                options = decode_options(option_bytes, &self.config, &headers).map_err(|e| { return e })?;
            } else if self.config.route_order == i {
                let route_bytes = src.split_to(headers.route_len as usize);
                route = String::from_utf8(route_bytes.into()).map_err(|_| ProtocolError::InvalidFrame("Invalid route"))?;
            } else if self.config.payload_order == i {
                payload = src.split_to(headers.payload_len as usize).freeze();
            }
        }

        return Ok(Some(Request {
            payload: payload,
            headers: headers,
            route: route,
            options: options,
        }));
    }
}

const MIN_OPTION_LENGTH: usize = 2 + 2 + 1 + 1;

fn calculate_options_length(options: &Vec<(String, String)>) -> Result<u32, ProtocolError> {
    let mut length = 0;
    for (key, value) in options {
        if key.len() > u16::MAX as usize {
            return Err(ProtocolError::InvalidFrame("Option key is too large"));
        }
        if value.len() > u16::MAX as usize {
            return Err(ProtocolError::InvalidFrame("Option value is too large"));
        }
        if key.is_empty() || value.is_empty() {
            return Err(ProtocolError::InvalidFrame("Option can be empty"));
        }
        length += 4 + key.len() + value.len();
    }
    Ok(length as u32)
}

fn encode_options(options: &Vec<(String, String)>, config: &FramerConfig, dst: &mut BytesMut) {
    for (key, value) in options {
        dst.put_u16(key.len() as u16);
        dst.put_u16(value.len() as u16);

        if config.options_key_first {
            dst.put_slice(key.as_bytes());
            dst.put_slice(value.as_bytes());
        } else {
            dst.put_slice(value.as_bytes());
            dst.put_slice(key.as_bytes());
        }
    }
}

fn decode_options(mut raw: BytesMut, config: &FramerConfig, headers: &RequestHeaders, ) -> Result<Vec<(String, String)>, ProtocolError> {
    if headers.options_count == 0 {
        if !raw.is_empty() {
            return Err(ProtocolError::InvalidFrame("Invalid options format"));
        }
        return Ok(vec![])
    }
    if raw.len() < MIN_OPTION_LENGTH {
        return Err(ProtocolError::InvalidFrame("Invalid options format"));
    }
    let mut options: Vec<(String, String)> = Vec::with_capacity(headers.options_count as usize);

    for _ in 1..=headers.options_count {
        if raw.len() < MIN_OPTION_LENGTH {
            return Err(ProtocolError::InvalidFrame("Invalid options format"));
        }
        let key_count: u16 = raw.get_u16();
        let value_count: u16 = raw.get_u16();

        let key: String;
        let value: String;

        if raw.len() < key_count as usize + value_count as usize {
            return Err(ProtocolError::InvalidFrame("Invalid options format"));
        }

        if config.options_key_first {
            let key_bytes = raw.split_to(key_count as usize);
            key = String::from_utf8(key_bytes.into()).map_err(|_| ProtocolError::InvalidFrame("Invalid options format"))?;
            let value_bytes = raw.split_to(value_count as usize);
            value = String::from_utf8(value_bytes.into()).map_err(|_| ProtocolError::InvalidFrame("Invalid options format"))?;
        } else {
            let value_bytes = raw.split_to(value_count as usize);
            value = String::from_utf8(value_bytes.into()).map_err(|_| ProtocolError::InvalidFrame("Invalid options format"))?;
            let key_bytes = raw.split_to(key_count as usize);
            key = String::from_utf8(key_bytes.into()).map_err(|_| ProtocolError::InvalidFrame("Invalid options format"))?;
        }
        options.push((key, value));
    }

    if !raw.is_empty() {
        return Err(ProtocolError::InvalidFrame("Trailing bytes in options"));
    }

    return Ok(options);
}

fn encode_headers(headers: &ResponseHeaders, options_count: u8, options_len: u32, payload_len: u32, config: &FramerConfig, dst: &mut BytesMut) -> Result<(), ProtocolError> {
    let opcode_bytes = match config.response_opcode_code.get(&headers.opcode).cloned() {
        Some(value) => value,
        None => {
            return Err(ProtocolError::InvalidFrame("Invalid opcode"));
        }
    };

    let hlen = config.header_length as usize;
    dst.resize(hlen, 0);

    dst[(config.version_pos - 1) as usize] = headers.version;
    dst[(config.opcode_pos - 1) as usize] = opcode_bytes;
    dst[(config.options_count_pos - 1) as usize] = options_count;
    dst[hlen - 12..hlen - 8].copy_from_slice(&options_len.to_be_bytes());
    dst[hlen - 8..hlen - 4].copy_from_slice(&payload_len.to_be_bytes());
    dst[hlen - 4..hlen].copy_from_slice(&headers.req_id.to_be_bytes());

    Ok(())
}

fn decode_headers(bytes: &[u8], config: &FramerConfig) -> Result<RequestHeaders, ProtocolError> {
    if bytes.len() < config.header_length as usize {
        return Err(ProtocolError::InvalidFrame("Headers slice is too short"));
    }

    let opcode: RequestOpcode;
    match bytes.get((config.opcode_pos - 1) as usize) {
        Some(&value) => {
            opcode = match config
                .request_opcode_code
                .get(value as usize)
                .cloned()
                .ok_or(ProtocolError::InvalidFrame("Invalid opcode"))? {
                Some(value) => value,
                None => return Err(ProtocolError::InvalidFrame("Invalid opcode")),
            }
        }
        None => {
            return Err(ProtocolError::InvalidFrame("Opcode not found"));
        }
    }

    let version: u8;
    match bytes.get((config.version_pos - 1) as usize) {
        Some(value) => {
            version = *value;
        }
        None => {
            return Err(ProtocolError::InvalidFrame("Version not found"));
        }
    }

    let route_len: u8;
    let route_bytes = bytes.get((config.route_len_pos - 1) as usize);
    match route_bytes {
        Some(value) => {
            route_len = *value;
        }
        None => {
            return Err(ProtocolError::InvalidFrame("Route length not found"));
        }
    }

    let options_count: u8;
    let options_count_bytes = bytes.get((config.options_count_pos - 1) as usize);
    match options_count_bytes {
        Some(value) => {
            options_count = *value;
        }
        None => {
            return Err(ProtocolError::InvalidFrame("Route length not found"));
        }
    }

    let mut big_bytes = &bytes[bytes.len() - 12..];

    let option_len = big_bytes.get_u32();
    let payload_len = big_bytes.get_u32();
    let req_id = big_bytes.get_u32();

    return Ok(RequestHeaders {
        payload_len: payload_len,
        options_count: options_count,
        version: version,
        opcode: opcode,
        route_len: route_len,
        req_id: req_id,
        options_len: option_len,
    });
}
