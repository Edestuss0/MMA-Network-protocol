#[derive(Debug, Clone)]
pub struct FramerConfig {
    pub max_message_length: u32,
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

pub const MIN_HEADER_LEN: u8 = 17;
pub const RESERVED_SUFFIX: u8 = 12;
impl FramerConfig {
    pub fn validate(&self) -> Result<(), ()> {
        if self.header_length < MIN_HEADER_LEN || self.header_length - RESERVED_SUFFIX < MIN_HEADER_LEN {
            return Err(());
        }
        let poses = [
            self.opcode_pos,
            self.version_pos,
            self.route_len_pos,
            self.options_count_pos,
        ];
        if poses.contains(&0) {
            return Err(());
        }
        for item in poses {
            if poses.iter().filter(|&&x| x == item).count() > 1
                || item > (self.header_length - RESERVED_SUFFIX)
            {
                return Err(());
            }
        }

        let orders = [self.payload_order, self.options_order, self.route_order];
        if orders.contains(&0) {
            return Err(());
        }
        for item in orders {
            if orders.iter().filter(|&&x| x == item).count() > 1 || item > 3 {
                return Err(());
            }
        }

        Ok(())
    }
    pub fn new() -> Self {
        Self {
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
