#![allow(unused)]

pub mod log_reader;
pub mod log_writer;

const BLOCK_SIZE: usize = 1 << 15; // 32KB

/// | crc32: 4B | len: 2B | type: 1B |
const HEADER_SIZE: usize = 4 + 2 + 1;

#[derive(Debug, Clone, Copy)]
pub enum RecordType {
    Zero = 0,
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl From<u8> for RecordType {
    fn from(value: u8) -> Self {
        match value {
            0 => RecordType::Zero,
            1 => RecordType::Full,
            2 => RecordType::First,
            3 => RecordType::Middle,
            4 => RecordType::Last,
            _ => panic!("Invalid record type, got {}", value),
        }
    }
}

// Report corruption in the log file
pub trait Reporter {
    fn corruption(&mut self, bytes: u64, reason: Box<dyn std::error::Error + Send + Sync>);
}
