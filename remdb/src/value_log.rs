#![allow(unused)]

use std::{hash::Hasher, mem, path::PathBuf, sync::Arc};

use bytes::{Buf, BufMut};

use crate::{
    error::{Error, Result},
    fs::File,
    options::DBOptions,
};

// meta on value first byte
struct Header {
    seq: u64,
    key_len: u32,
    value_len: u32,
}

const VLOG_HEADER_SIZE: usize =
    mem::size_of::<u64>() + mem::size_of::<u32>() + mem::size_of::<u32>();

impl Header {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u64_le(self.seq);
        buf.put_u32_le(self.key_len);
        buf.put_u32_le(self.value_len);
    }

    pub fn decode(&self, mut buf: &[u8]) -> Result<Self> {
        if buf.len() <= VLOG_HEADER_SIZE {
            return Err(Error::Decode(format!(
                "header size is not enough: {}",
                buf.len()
            )));
        }

        let seq = buf.get_u64_le();
        let key_len = buf.get_u32_le();
        let value_len = buf.get_u32_le();

        Ok(Self {
            seq,
            key_len,
            value_len,
        })
    }
}

pub struct ValueLog {
    file: File,

    write_offset: u32,
    buf: Vec<u8>,
}

impl ValueLog {
    pub async fn open(file: File) -> Result<Self> {
        let write_offset = file.len().await? as u32;

        Ok(Self {
            file,
            write_offset,
            buf: Vec::with_capacity(VLOG_HEADER_SIZE),
        })
    }

    pub async fn put(&mut self, seq: u64, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_batch(std::iter::once((seq, key, value))).await
    }

    pub async fn put_batch<'a>(
        &mut self,
        iter: impl Iterator<Item = (u64, &'a [u8], &'a [u8])>,
    ) -> Result<()> {
        self.load_buf(iter);

        let buf_len = self.buf.len();

        let offset = self.write_offset as u64;
        let end_offset = self.write_offset as u64 + buf_len as u64;

        self.file.write_all_at(&self.buf, offset).await?;
        self.file.sync_range(offset, buf_len).await?;

        self.write_offset += buf_len as u32;

        Ok(())
    }

    fn load_buf<'a>(&mut self, iter: impl Iterator<Item = (u64, &'a [u8], &'a [u8])>) {
        self.buf.clear();

        for (seq, key, value) in iter {
            Self::load_one_entry(&mut self.buf, seq, key, value);
        }
    }

    fn load_one_entry(buf: &mut Vec<u8>, seq: u64, key: &[u8], value: &[u8]) {
        let header = Header {
            seq,
            key_len: key.len() as u32,
            value_len: value.len() as u32,
        };

        let mut hasher = crc32fast::Hasher::new();
        header.encode(buf);
        hasher.update(&buf[buf.len() - VLOG_HEADER_SIZE..]);
        buf.extend_from_slice(key);
        hasher.update(key);
        buf.extend_from_slice(value);
        hasher.update(value);

        let checksum = hasher.finalize();
        buf.put_u32_le(checksum);
    }

    pub fn into_file(self) -> File {
        self.file
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use crate::fs::IoManager;

    use super::ValueLog;

    #[tokio::test]
    async fn write_some_data() -> anyhow::Result<()> {
        let tempfile = tempfile::tempfile()?;

        let context = IoManager::new()?;
        let file = context.open_file_from_fd(tempfile);

        let mut vlog = ValueLog::open(file).await?;

        let keys = vec!["key1", "key2", "key3"];
        let values = vec!["value1", "value2", "value3"];

        for (i, (key, value)) in keys.iter().zip(values.iter()).enumerate() {
            vlog.put(i as u64, key.as_bytes(), value.as_bytes()).await?;
        }

        let file = vlog.into_file();

        let actual = {
            let len = file.len().await? as usize;
            let mut buf = vec![0; len];
            file.read_exact_at(&mut buf, 0).await?;
            buf
        };

        let expected = {
            let mut buf: Vec<u8> = vec![];
            for (i, (key, value)) in keys.iter().zip(values.iter()).enumerate() {
                let header = super::Header {
                    seq: i as u64,
                    key_len: key.len() as u32,
                    value_len: value.len() as u32,
                };

                let mut hasher = crc32fast::Hasher::new();
                header.encode(&mut buf);
                hasher.update(&buf[buf.len() - super::VLOG_HEADER_SIZE..]);
                buf.extend_from_slice(key.as_bytes());
                hasher.update(key.as_bytes());
                buf.extend_from_slice(value.as_bytes());
                hasher.update(value.as_bytes());

                let checksum = hasher.finalize();
                buf.put_u32_le(checksum);
            }
            buf
        };

        assert_eq!(expected, actual);

        Ok(())
    }
}
