use std::collections::HashMap;
use crate::protocol::commands::{RequestOpcode, ResponseOpcode};
use crate::protocol::framer::RequestOpcode::{Channel, Once};

#[derive(Debug, Clone)]
pub struct FramerConfig {
    pub max_message_length: u32,
    pub request_opcode_code: [Option<RequestOpcode>; 256],
    pub response_opcode_code: HashMap<ResponseOpcode, u8>,
    pub opcode_pos: u8,
    pub version_pos: u8,
    pub route_len_pos: u8,
    pub route_order: u8,
    pub payload_order: u8,
    pub options_order: u8,
    pub options_count_pos: u8,
    pub options_key_first: bool,
    pub header_length: u8,
}

const MIN_HEADER_LEN: u8 = 13;
const RESERVED_SUFFIX: u8 = 12;
impl FramerConfig {
    pub fn validate(&self) -> Result<(), ()> {
        if self.header_length < MIN_HEADER_LEN {
            return Err(());
        }
        let poses = [self.opcode_pos, self.version_pos, self.route_len_pos, self.options_count_pos];
        if poses.contains(&0) {
            return Err(());
        }
        for item in poses {
            if poses.iter().filter(|&&x| x == item).count() > 1 || item >= (self.header_length - RESERVED_SUFFIX) {
                return Err(());
            }
        }

        let orders = [self.payload_order, self.options_order, self.route_order];
        if orders.contains(&0) {
            return Err(());
        }
        for item in orders {
            if orders.iter().filter(|&&x| x == item).count() > 1 || item > 3  {
                return Err(());
            }
        }

        if self.request_opcode_code.is_empty() || self.response_opcode_code.is_empty() || self.request_opcode_code.iter().all(|o| o.is_none()) {return Err(())}

        Ok(())
    }
    pub fn new() -> Self {
        let mut req_opcodes = [None; 256];
        req_opcodes[1] = Some(Channel);
        req_opcodes[2] = Some(Once);
        Self {
            request_opcode_code: req_opcodes,
            response_opcode_code: HashMap::from([(ResponseOpcode::Ok, 1), (ResponseOpcode::BadRequest, 2), (ResponseOpcode::Conflict, 3), (ResponseOpcode::Forbidden, 4), (ResponseOpcode::InternalError, 5), (ResponseOpcode::Unauthorized, 6), (ResponseOpcode::Message, 7), (ResponseOpcode::NotFound, 8)]),
            opcode_pos: 1,
            version_pos: 2,
            max_message_length: 10 * 1000 * 1000,
            route_len_pos: 3,
            options_count_pos: 4,
            route_order: 1,
            options_order: 2,
            payload_order: 3,
            options_key_first: true,
            header_length: MIN_HEADER_LEN,
        }
    }
}