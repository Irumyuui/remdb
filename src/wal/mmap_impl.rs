use core::mem::{self, ManuallyDrop};
use std::{
    option,
    path::{Path, PathBuf},
    ptr,
    sync::Arc,
};

use bytes::{Buf, BufMut, BytesMut};
use memmap2::{MmapMut, MmapOptions};

use crate::{
    db_options::DBOptions,
    error::{Error, Result},
};

struct Header {
    checksum: u32,
    key_len: u32,
    value_len: u32,
    seq: u64,
    // meta: u8,  // consider later
}

const VLOG_HEADER_SIZE: usize =
    mem::size_of::<u32>() + mem::size_of::<u32>() + mem::size_of::<u32>() + mem::size_of::<u64>(); // + mem::size_of::<u8>();

impl Header {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(self.checksum);
        buf.put_u32_le(self.key_len);
        buf.put_u32_le(self.value_len);
        buf.put_u64_le(self.seq);
        // buf.put_u8(self.meta);
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() <= VLOG_HEADER_SIZE {
            return Err(Error::Decode("header size is not enough".into()));
        }

        let checksum = buf.get_u32_le();
        let key_len = buf.get_u32_le();
        let value_len = buf.get_u32_le();
        let seq = buf.get_u64_le();

        // let meta = buf.get_u8();
        Ok(Self {
            checksum,
            key_len,
            value_len,
            seq,
            // meta,
        })
    }
}

pub struct Wal {
    file: ManuallyDrop<std::fs::File>,
    mmap: ManuallyDrop<MmapMut>,
    path: PathBuf,

    write_offset: u32,
    buf: Vec<u8>,
    size: usize,

    opts: Arc<DBOptions>,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>, options: Arc<DBOptions>) -> Result<Self> {
        let file_exists = path.as_ref().exists();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        if !file_exists {
            file.set_len(options.wal_log_size as u64 * 3)?; // is it ?
            file.sync_all()?;
            std::fs::File::open(path.as_ref().parent().unwrap())?.sync_all()?;
        }

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        let mut this = Self {
            size: mmap.len(),

            file: ManuallyDrop::new(file),
            mmap: ManuallyDrop::new(mmap),
            path: path.as_ref().to_path_buf(),

            write_offset: 0,
            buf: Vec::new(),

            opts: options,
        };

        if !file_exists {
            this.bootstrap()?;
        }

        Ok(this)
    }

    fn bootstrap(&mut self) -> Result<()> {
        self.pending_zero_next_entry()
    }

    fn pending_zero_next_entry(&mut self) -> Result<()> {
        let zero_header = &mut self.mmap
            [self.write_offset as usize..self.write_offset as usize + VLOG_HEADER_SIZE];
        unsafe {
            ptr::write_bytes(zero_header.as_mut_ptr(), 0, zero_header.len());
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    pub fn truncate(&mut self, target_size: u64) -> Result<()> {
        let len = self.file.metadata()?.len();
        if len == target_size {
            return Ok(());
        }

        self.size = target_size as _;
        self.file.set_len(target_size)?;
        self.file.sync_all()?;

        Ok(())
    }

    fn finish_internal(&mut self, size: u64) -> Result<()> {
        self.file.sync_all()?;
        self.truncate(size)?;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.finish_internal(self.size as u64)?;
        Ok(())
    }

    pub fn put(&mut self, seq: u64, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_batch([(seq, key, value)].iter().cloned())
    }

    pub fn put_batch<'a>(
        &'a mut self,
        data: impl Iterator<Item = (u64, &'a [u8], &'a [u8])>,
    ) -> Result<()> {
        for (seq, k, v) in data {
            Self::reload_entry(&mut self.buf, seq, k, v);
            self.mmap[self.write_offset as usize..self.write_offset as usize + self.buf.len()]
                .copy_from_slice(&self.buf);
            self.write_offset += self.buf.len() as u32;
            self.pending_zero_next_entry()?;
        }

        // self.sync()?;

        Ok(())
    }

    fn reload_entry(buf: &mut Vec<u8>, seq: u64, key: &[u8], value: &[u8]) -> Result<()> {
        buf.clear();

        buf.reserve(VLOG_HEADER_SIZE + key.len() + value.len());

        let header = Header {
            checksum: 0,
            key_len: key.len() as _,
            value_len: value.len() as _,
            seq,
        };
        header.encode(buf);
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);

        let check_sum = crc32fast::hash(&buf[4..]);
        buf[..].as_mut().put_u32_le(check_sum);

        Ok(())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.mmap);
            ManuallyDrop::drop(&mut self.file);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Read, sync::Arc};

    use bytes::BufMut;
    use remdb_fs::traits::File;
    use scopeguard::defer;

    use crate::{
        db_options::{DBOpenOptions, DBOptions},
        wal::mmap_impl::{Header, VLOG_HEADER_SIZE, Wal},
    };

    #[test]
    fn write_some_data() -> anyhow::Result<()> {
        let test_path = std::env::temp_dir().join("mmap_wal_test.txt");
        defer! {
            let _ = std::fs::remove_dir(test_path.clone());
        }

        let mut wal = Wal::open(&test_path, DBOpenOptions::default().build())?;
        wal.put(0, b"hello", b"world")?;
        wal.sync()?;
        wal.finish()?;
        drop(wal);

        let mut expected: Vec<u8> = Vec::new();
        Header {
            checksum: 0,
            key_len: 5,
            value_len: 5,
            seq: 0,
        }
        .encode(&mut expected);
        expected.extend_from_slice(b"hello");
        expected.extend_from_slice(b"world");

        let c = crc32fast::hash(&expected[4..]);
        expected[..].as_mut().put_u32_le(c);

        let mut file = std::fs::OpenOptions::new().read(true).open(&test_path)?;
        let mut actual = vec![0; VLOG_HEADER_SIZE + 5 + 5];
        file.read_exact(&mut actual);

        assert_eq!(expected, actual);

        Ok(())
    }
}
