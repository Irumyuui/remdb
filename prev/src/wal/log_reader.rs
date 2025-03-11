use std::io::SeekFrom;

use bytes::Buf;
use remdb_fs::traits::File;

use super::{BLOCK_SIZE, HEADER_SIZE, RecordType, Reporter};

#[derive(Debug)]
enum ReadError {
    Eof,
    BadRecord,
}

pub struct LogReader<F: File> {
    file: F,
    reporter: Option<Box<dyn Reporter>>,

    checksum: bool,
    initial_offset: u64,

    last_record_offset: u64,
    eof: bool,

    buf: Vec<u8>,
    buf_pointer: usize,
    buf_len: usize,
    resyncing: bool,

    end_of_buf_offset: u64,
}

impl<F: File> LogReader<F> {
    pub fn new(
        file: F,
        reporter: Option<Box<dyn Reporter>>,
        checksum: bool,
        initial_offset: u64,
    ) -> Self {
        Self {
            file,
            reporter,
            checksum,
            initial_offset,
            buf: vec![0; BLOCK_SIZE],
            buf_pointer: 0,
            buf_len: 0,
            eof: false,
            last_record_offset: 0,
            end_of_buf_offset: 0,
            resyncing: initial_offset > 0,
        }
    }

    pub fn read_record(&mut self, buf: &mut Vec<u8>) -> bool {
        if self.last_record_offset < self.initial_offset && !self.skip_to_initial_block() {
            return false;
        }

        let mut in_fragmented_record = false;
        let mut prospective_record_offset = 0;
        loop {
            match self.read_physical_record() {
                Ok((rec_ty, mut payload)) => {
                    if self.resyncing {
                        match rec_ty {
                            RecordType::Middle => continue,
                            RecordType::Last => {
                                self.resyncing = false;
                                continue;
                            }
                            _ => self.resyncing = false,
                        }
                    }

                    let framge_len = payload.len();
                    let physical_record_offset = self.end_of_buf_offset
                        - self.buf_len as u64
                        - HEADER_SIZE as u64
                        - framge_len as u64;

                    match rec_ty {
                        RecordType::Full => {
                            if in_fragmented_record {
                                // in mid, wtf?
                                self.report_corruption(
                                    buf.len() as _,
                                    "is it mid? but found a full".into(),
                                );
                            }

                            self.last_record_offset = physical_record_offset;
                            buf.clear();
                            buf.append(&mut payload);
                            return true;
                        }
                        RecordType::First => {
                            if in_fragmented_record {
                                // wtf?
                                self.report_corruption(
                                    buf.len() as _,
                                    "is it mid? but found a first".into(),
                                );
                            }

                            prospective_record_offset = physical_record_offset;
                            buf.clear();
                            buf.append(&mut payload);
                            in_fragmented_record = true;
                        }
                        RecordType::Middle => {
                            if !in_fragmented_record {
                                self.report_corruption(
                                    framge_len as _,
                                    "is it not a mid? but found a middle".into(),
                                );
                            } else {
                                buf.append(&mut payload);
                            }
                        }
                        RecordType::Last => {
                            if !in_fragmented_record {
                                self.report_corruption(
                                    buf.len() as _,
                                    "is it not a mid? but found a last".into(),
                                );
                            } else {
                                buf.append(&mut payload);
                                self.last_record_offset = prospective_record_offset;
                                return true;
                            }
                        }
                        _ => {
                            // wtf?
                        }
                    }
                }
                Err(e) => match e {
                    ReadError::Eof => {
                        if in_fragmented_record {
                            buf.clear();
                        }
                        return false;
                    }
                    ReadError::BadRecord => {
                        if in_fragmented_record {
                            self.report_corruption(buf.len() as _, "bad record".into());
                        }
                        in_fragmented_record = false;
                        buf.clear();
                    }
                },
            }
        }
    }

    fn read_physical_record(&mut self) -> Result<(RecordType, Vec<u8>), ReadError> {
        loop {
            if self.buf_len < HEADER_SIZE {
                self.clear_buf();
                if !self.eof {
                    match self.file.read(&mut self.buf) {
                        Ok(read_size) => {
                            self.end_of_buf_offset += read_size as u64;
                            self.buf_len = read_size;
                            if read_size < BLOCK_SIZE {
                                self.eof = true;
                            }
                        }
                        Err(e) => {
                            self.report_corruption(BLOCK_SIZE as _, e.into());
                            self.eof = true;
                            return Err(ReadError::Eof);
                        }
                    }
                    continue;
                } else {
                    return Err(ReadError::Eof);
                }
            }

            let header = &self.buf[0..HEADER_SIZE];
            let rec_ty = header[HEADER_SIZE - 1];
            let payload_len = header[4..].as_ref().get_u16_le() as usize;
            let record_len = HEADER_SIZE + payload_len;

            if record_len > self.buf_len {
                let buf_len = self.buf_len;
                self.clear_buf();

                if !self.eof {
                    self.report_corruption(self.buf_len as _, "bad record len".into());
                    return Err(ReadError::BadRecord);
                }
                return Err(ReadError::Eof);
            }

            if rec_ty == 0 && payload_len == 0 {
                self.clear_buf();
                self.report_corruption(self.buf.len() as _, "empty record len".into());
                return Err(ReadError::BadRecord);
            }

            // crc
            if self.checksum {
                let read_crc = header[..4].as_ref().get_u32_le();
                let actual = crc32fast::hash(&self.buf[4..record_len]);
                if read_crc != actual {
                    let buf_len = self.buf_len;
                    self.clear_buf();
                    self.report_corruption(buf_len as _, "crc mismatch".into());
                    return Err(ReadError::BadRecord);
                }
            }

            let mut payload: Vec<_> = self.buf.drain(0..record_len).collect();
            self.buf_len -= record_len;

            if self.end_of_buf_offset
                < self.initial_offset + self.buf_len as u64 + record_len as u64
            {
                return Err(ReadError::BadRecord);
            }

            payload.drain(0..HEADER_SIZE);
            return Ok((RecordType::from(rec_ty), payload));
        }
    }

    fn clear_buf(&mut self) {
        // self.buf_pointer = 0;
        self.buf = vec![0; BLOCK_SIZE];
        self.buf_len = 0;
    }

    fn report_corruption(&mut self, bytes: u64, reason: Box<dyn std::error::Error + Send + Sync>) {
        if let Some(reporter) = self.reporter.as_mut() {
            reporter.corruption(bytes, reason);
        }
    }

    fn skip_to_initial_block(&mut self) -> bool {
        let offset_in_block = self.initial_offset % BLOCK_SIZE as u64;
        let mut block_start = self.initial_offset - offset_in_block;

        if offset_in_block > BLOCK_SIZE as u64 - 6 {
            block_start = BLOCK_SIZE as u64;
        }
        self.end_of_buf_offset = block_start;

        if block_start > 0
            && let Err(e) = self.file.seek(SeekFrom::Start(block_start))
        {
            self.report_corruption(block_start, e.into());
            return false;
        }

        true
    }

    pub fn into_file(self) -> F {
        self.file
    }
}
