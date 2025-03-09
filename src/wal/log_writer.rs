use std::io;

use bytes::BufMut;
use remdb_fs::traits::File;

use crate::wal::{BLOCK_SIZE, HEADER_SIZE, RecordType};

const PANDING: [u8; HEADER_SIZE] = [0; HEADER_SIZE];

pub struct LogWriter<F: File> {
    file: F,
    block_offset: usize,
}

impl<F: File> LogWriter<F> {
    pub fn new(file: F) -> Self {
        Self {
            file,
            block_offset: 0,
        }
    }

    pub fn add_record(&mut self, data: &[u8]) -> io::Result<()> {
        let mut is_begin = true;
        let mut offset = 0;

        loop {
            assert!(
                self.block_offset <= BLOCK_SIZE,
                "block_offset > BLOCK_SIZE, block_offset: {}, BLOCK_SIZE: {}",
                self.block_offset,
                BLOCK_SIZE
            );

            let remain = BLOCK_SIZE - self.block_offset;
            if remain < HEADER_SIZE {
                if remain > 0 {
                    self.file.write(&PANDING[..remain])?;
                }
                self.block_offset = 0;
            }

            let data_len = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let write_len = (data.len() - offset).min(data_len);
            assert!(offset + write_len <= data.len());
            let is_end = offset + write_len == data.len();

            let rec_ty = if is_begin && is_end {
                RecordType::Full
            } else if is_begin {
                RecordType::First
            } else if is_end {
                RecordType::Last
            } else {
                RecordType::Middle
            };

            let data_slice = &data[offset..offset + write_len];
            self.write_slice(rec_ty, data_slice)?;
            offset += data_len;
            is_begin = false;

            if is_end {
                break;
            }
        }

        Ok(())
    }

    fn write_slice(&mut self, rec_ty: RecordType, slice: &[u8]) -> io::Result<()> {
        let len = slice.len();
        assert!(len <= BLOCK_SIZE - HEADER_SIZE);

        let mut header = [0_u8; HEADER_SIZE];
        let mut buf = &mut header[..];
        header[4..].as_mut().put_u16_le(len as _);
        header[6] = rec_ty as u8;

        let mut crc_hasher = crc32fast::Hasher::new();
        crc_hasher.update(&header[4..]);
        crc_hasher.update(slice);
        let crc = crc_hasher.finalize();
        header[..].as_mut().put_u32_le(crc);

        self.file.write(&header);
        self.file.write(slice)?;
        self.block_offset += HEADER_SIZE + len;

        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
