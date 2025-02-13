#![allow(unused)]

use std::{
    fs::File,
    path::{Path, PathBuf},
};

use bytes::{Buf, BufMut, BytesMut};
use memmap2::{MmapMut, MmapOptions};

use crate::{
    entry::Entry,
    error::{Error, Result},
};

const HEADER_SIZE: usize = std::mem::size_of::<u32>() * 2 + std::mem::size_of::<u8>();

#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub key_len: u32,
    pub value_len: u32,
    pub meta: u8, // reserve
}

impl Header {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.meta);
        buf.put_u32_le(self.key_len);
        buf.put_u32_le(self.value_len);
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < HEADER_SIZE {
            return Err(Error::VLogDecode(format!(
                "buf not enough, expect: {}, actual: {}",
                HEADER_SIZE,
                buf.remaining()
            )));
        }

        let meta = buf.get_u8();
        let key_len = buf.get_u32_le();
        let value_len = buf.get_u32_le();

        Ok(Self {
            key_len,
            value_len,
            meta,
        })
    }
}

pub struct LogFile {
    path: PathBuf,
    file: File,
    mmap: MmapMut,
    write_at: usize,
    buf: BytesMut,
}

impl LogFile {
    fn open_or_create_file(path: impl AsRef<Path>, vlog_file_size: usize) -> Result<(File, bool)> {
        if path.as_ref().exists() {
            return Ok((
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(false)
                    .open(path)?,
                false,
            ));
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        file.set_len(vlog_file_size as u64)?;
        file.sync_all()?;
        std::fs::File::open(path.as_ref().parent().unwrap())?.sync_all()?;
        Ok((file, true))
    }

    pub(crate) fn open(path: impl AsRef<Path>, vlog_file_size: usize) -> Result<Self> {
        let (file, is_created) = Self::open_or_create_file(path.as_ref(), vlog_file_size)?;

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        let mut vlog = Self {
            path: path.as_ref().into(),
            file,
            mmap,
            write_at: 0, // zero? or next entry? or set vlog header? if crash, how to recover?
            buf: BytesMut::new(),
        };

        if is_created {
            vlog.zero_next_entry();
        }

        todo!()
    }

    pub(crate) fn write_entry(&mut self, entry: &Entry) -> Result<()> {
        self.buf.clear();
        Self::encode_entry(&mut self.buf, entry);
        self.mmap[self.write_at..self.write_at + self.buf.len()].copy_from_slice(&self.buf);
        self.write_at += self.buf.len();
        self.zero_next_entry();
        Ok(())
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    pub(crate) fn zero_next_entry(&mut self) {
        self.mmap[self.write_at..self.write_at + HEADER_SIZE].copy_from_slice(&[0u8; HEADER_SIZE]);
    }

    pub(crate) fn encode_entry(buf: &mut BytesMut, entry: &Entry) {
        let header = Header {
            key_len: entry.key.len() as _,
            value_len: entry.value.len() as _,
            meta: entry.meta,
        };

        header.encode(buf);
        buf.put(entry.key.as_ref());
        buf.put(entry.value.as_ref());

        // TODO: use flag
        let check_sum = crc32fast::hash(buf);
        buf.put_u32_le(check_sum);
    }
}
