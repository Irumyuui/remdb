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

#[cfg(test)]
mod tests {
    use remdb_fs::{memory::MemFileSystem, traits::FileSystem};

    use super::{Reporter, log_reader::LogReader, log_writer::LogWriter};

    struct MockReporter;

    impl Reporter for MockReporter {
        fn corruption(&mut self, bytes: u64, reason: Box<dyn std::error::Error + Send + Sync>) {
            unimplemented!()
        }
    }

    #[test]
    fn test_small_but_full_data() -> anyhow::Result<()> {
        let inputs = vec![b"hello world".to_vec(), b"fuck".to_vec()];
        let mem_fs = MemFileSystem::default();

        let file = mem_fs.create("wal.log")?;
        dbg!(&file);

        let mut writer = LogWriter::new(file);
        for s in inputs.iter() {
            writer.add_record(&s[..])?;
        }
        drop(writer);

        let file = mem_fs.open("wal.log")?;
        let mut reader = LogReader::new(file, Some(Box::new(MockReporter)), true, 0);

        let mut output = Vec::new();
        let mut buf = Vec::new();
        while reader.read_record(&mut buf) {
            output.push(buf.clone());
            buf.clear();
        }

        assert_eq!(output.len(), 2);
        for (expected, actual) in inputs.iter().zip(output.iter()) {
            assert_eq!(expected, actual);
        }

        Ok(())
    }
}
