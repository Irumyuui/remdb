#![allow(unused)]

use std::{
    collections::HashMap,
    fs::OpenOptions,
    hash::Hasher,
    mem,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use bytes::{Buf, BufMut};
use fast_async_mutex::rwlock::RwLock;

use crate::{
    error::{Error, Result},
    format::{VLOF_FILE_SUFFIX, vlog_format_path},
    fs::File,
    key::KeySlice,
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

pub struct ValueLogFile {
    file: File,
    write_offset: u32,
    buf: Vec<u8>,
}

impl ValueLogFile {
    pub async fn open(file: File) -> Result<Self> {
        let write_offset = file.len().await? as u32;

        Ok(Self {
            file,
            write_offset,
            buf: Vec::with_capacity(VLOG_HEADER_SIZE),
        })
    }

    pub async fn put(&mut self, key: KeySlice<'_>, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)]).await
    }

    pub async fn put_batch<'a>(&mut self, data: &[(KeySlice<'a>, &'a [u8])]) -> Result<()> {
        self.load_buf(data);

        let buf_len = self.buf.len();

        let offset = self.write_offset as u64;
        let end_offset = self.write_offset as u64 + buf_len as u64;

        self.file.write_all_at(&self.buf, offset).await?;
        self.file.sync_range(offset, buf_len).await?;

        self.write_offset += buf_len as u32;

        Ok(())
    }

    fn load_buf<'a>(&mut self, data: &[(KeySlice<'a>, &'a [u8])]) {
        self.buf.clear();

        for (key, value) in data {
            Self::load_one_entry(&mut self.buf, key.seq(), key.key(), value);
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

// TODO: add deleted vlog file, just deleted?
pub struct ValueLogInner {
    vlogs: HashMap<u32, Arc<RwLock<ValueLogFile>>>,
    max_fid: u32,
}

impl ValueLogInner {
    fn new() -> Self {
        Self {
            vlogs: HashMap::new(),
            max_fid: 0,
        }
    }
}

pub struct ValueLog {
    inner: Arc<RwLock<ValueLogInner>>,
    options: Arc<DBOptions>,

    write_offset: AtomicU32,
}

impl ValueLog {
    pub async fn new(options: Arc<DBOptions>) -> Result<Self> {
        let mut this = Self {
            inner: Arc::new(RwLock::new(ValueLogInner::new())),
            options,
            write_offset: AtomicU32::new(0),
        };

        this.restart_value_log().await?;

        Ok(this)
    }

    async fn restart_value_log(&self) -> Result<()> {
        self.open_value_log_dir().await?;
        if !self.restart_as_old_vlog_file().await? {
            self.create_vlog_file().await?;
        }
        Ok(())
    }

    async fn open_value_log_dir(&self) -> Result<()> {
        // search all value log files
        let dir = std::fs::read_dir(&self.options.vlog_dir_path)?;
        let mut inner = self.inner.write().await;
        for file in dir {
            let file = file?;
            let file_name = file.file_name().into_string().map_err(|e| {
                Error::Corruption(format!("invalid value log name: {:?}", e).into())
            })?;
            if !file_name.ends_with(VLOF_FILE_SUFFIX) {
                continue;
            }

            let fid = file_name[..file_name.len() - VLOF_FILE_SUFFIX.len()]
                .parse::<u32>()
                .map_err(|e| {
                    Error::Corruption(
                        format!("invalid value log name, parse fid failed: {:?}", e).into(),
                    )
                })?;

            let file_path = file.path();
            let mut open_options = std::fs::OpenOptions::new();
            open_options.create(false).read(true).write(true);
            let file = self.options.io_manager.open_file(file_path, open_options)?;
            let vlog_file = Arc::new(RwLock::new(ValueLogFile::open(file).await?));

            if inner.vlogs.insert(fid, vlog_file).is_some() {
                return Err(Error::Corruption(
                    format!("fid {} already exists", fid).into(),
                ));
            }
            inner.max_fid = inner.max_fid.max(fid);
        }

        Ok(())
    }

    async fn restart_as_old_vlog_file(&self) -> Result<bool> {
        let inner = self.inner.read().await;
        if let Some(last_file) = inner.vlogs.get(&inner.max_fid) {
            let file = last_file.read().await;
            if file.write_offset < self.options.vlog_size as u32 {
                self.write_offset.store(file.write_offset, Ordering::SeqCst);
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn create_vlog_file(&self) -> Result<()> {
        let mut inner = self.inner.write().await;
        let next_fid = inner.max_fid + 1;
        let path = vlog_format_path(&self.options.vlog_dir_path, next_fid);
        let mut opts = OpenOptions::new();
        opts.create(true).read(true).write(true);
        let fd = self.options.io_manager.open_file(path, opts)?;
        let vlog_file = Arc::new(RwLock::new(ValueLogFile::open(fd).await?));
        assert!(inner.vlogs.insert(next_fid, vlog_file).is_none());
        inner.max_fid = next_fid;
        self.write_offset.store(0, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use crate::{fs::IoManager, key::KeySlice};

    use super::ValueLogFile;

    #[tokio::test]
    async fn write_some_data() -> anyhow::Result<()> {
        let tempfile = tempfile::tempfile()?;

        let context = IoManager::new()?;
        let file = context.open_file_from_fd(tempfile);

        let mut vlog = ValueLogFile::open(file).await?;

        let keys = vec!["key1", "key2", "key3"];
        let values = vec!["value1", "value2", "value3"];

        for (i, (key, value)) in keys.iter().zip(values.iter()).enumerate() {
            vlog.put(KeySlice::new(key.as_bytes(), i as _), value.as_bytes())
                .await?;
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
