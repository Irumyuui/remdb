#![allow(unused)]

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use fast_async_mutex::mutex::Mutex;

use crate::{
    error::{Error::Corruption, Result},
    fs::File,
};

#[derive(Debug, Default)]
struct Actions {
    wals: Vec<u32>,
    tables: Vec<(u32, u32)>,
    value_logs: Vec<u32>,
}

impl Actions {
    fn add_record(&mut self, record: Record) {
        match record {
            Record::Wal { id } => self.wals.push(id),
            Record::Table { level, id } => self.tables.push((level, id)),
            Record::ValueLog { id } => self.value_logs.push(id),
        }
    }

    fn clear(&mut self) {
        self.wals.clear();
        self.tables.clear();
        self.value_logs.clear();
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.wals.len() as u32);
        for id in self.wals.iter() {
            buf.put_u32_le(*id);
        }

        buf.put_u32_le(self.tables.len() as u32);
        for (level, id) in self.tables.iter() {
            buf.put_u32_le(*level);
            buf.put_u32_le(*id);
        }

        buf.put_u32_le(self.value_logs.len() as u32);
        for id in self.value_logs.iter() {
            buf.put_u32_le(*id);
        }
    }

    fn decode(mut buf: &[u8]) -> (Self, &[u8]) {
        let wal_len = buf.get_u32_le();
        let mut wals = Vec::with_capacity(wal_len as usize);
        for _ in 0..wal_len {
            wals.push(buf.get_u32_le());
        }

        let table_len = buf.get_u32_le();
        let mut tables = Vec::with_capacity(table_len as usize);
        for _ in 0..table_len {
            tables.push((buf.get_u32_le(), buf.get_u32_le()));
        }

        let value_log_len = buf.get_u32_le();
        let mut value_logs = Vec::with_capacity(value_log_len as usize);
        for _ in 0..value_log_len {
            value_logs.push(buf.get_u32_le());
        }

        (
            Self {
                wals,
                tables,
                value_logs,
            },
            buf,
        )
    }

    fn is_empty(&self) -> bool {
        self.wals.is_empty() && self.tables.is_empty() && self.value_logs.is_empty()
    }

    fn emit_size(&self) -> usize {
        4 + self.wals.len() * 4  // wal 
        + 4 + self.tables.len() * 8  // table 
        + 4 + self.value_logs.len() * 4 // value log
    }
}

#[derive(Debug, Default)]
struct Version {
    add_actions: Actions,
    del_actions: Actions,
}

impl Version {
    fn add_action(&mut self, action: Action) {
        match action {
            Action::Add(record) => self.add_actions.add_record(record),
            Action::Delete(record) => self.del_actions.add_record(record),
        }
    }

    fn emit_size(&self) -> usize {
        self.add_actions.emit_size() + self.del_actions.emit_size()
    }

    fn is_empty(&self) -> bool {
        self.add_actions.is_empty() && self.del_actions.is_empty()
    }

    fn clear(&mut self) {
        self.add_actions.clear();
        self.del_actions.clear();
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.add_actions.encode(buf);
        self.del_actions.encode(buf);
    }

    // because is checked by checksum, so if some error, just panic
    fn decode(buf: &[u8]) -> Self {
        todo!()
    }
}

/// Format:
///
/// ```text
///     +---------------+-------------+----------------+-------+
///     | manifest size | add records | delete records | crc32 |
///     +---------------+-------------+----------------+-------+
/// ```
struct ManifestFile {
    file: File,
    write_offset: u64,

    current_version: Version,

    buf: BytesMut,
}

impl ManifestFile {
    // TODO: should recover
    async fn new(file: File, offset: u64) -> Result<Self> {
        Ok(Self {
            file,
            write_offset: offset, // append
            current_version: Version::default(),
            buf: BytesMut::new(),
        })
    }

    fn add_action(&mut self, action: Action) {
        self.current_version.add_action(action);
    }

    fn emit_size(&self) -> usize {
        self.current_version.emit_size()
    }

    fn is_empty(&self) -> bool {
        self.current_version.is_empty()
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.current_version.clear();
    }

    async fn flush(&mut self) -> Result<()> {
        assert!(!self.is_empty(), "should not flush empty manifest");

        self.buf.put_u32_le(self.emit_size() as u32);
        self.current_version.encode(&mut self.buf);
        self.buf.put_u32_le(crc32fast::hash(&self.buf));

        self.file.write_all_at(&self.buf, self.write_offset).await?;
        self.write_offset += self.buf.len() as u64;

        self.clear();

        Ok(())
    }
}

pub enum Record {
    Wal { id: u32 },
    Table { level: u32, id: u32 },
    ValueLog { id: u32 },
}

pub enum Action {
    Add(Record),
    Delete(Record),
}

// `last_seq` should recorver from search file...
pub struct Manifest {
    file: Arc<Mutex<ManifestFile>>,
}

impl Manifest {
    async fn new(file: File, offset: u64) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(ManifestFile::new(file, offset).await?)),
        })
    }

    pub async fn add_action(&self, action: Action) {
        self.file.lock().await.add_action(action);
    }

    pub async fn flush(&self) -> Result<()> {
        self.file.lock().await.flush().await
    }

    async fn recover_from_file(file: File) -> Result<(Self, Vec<Version>)> {
        let file_size = file.len().await?;
        if file_size < 4 {
            return Ok((
                Self {
                    file: Arc::new(Mutex::new(ManifestFile::new(file, 0).await?)),
                },
                vec![],
            ));
        }

        let mut left = 0;
        let mut versions = vec![];
        let mut buf = BytesMut::new();
        loop {
            if left + 4 <= file_size {
                // drop tail;
                tracing::warn!("manifest file has some tail data, will drop it.");
                break;
            }

            buf.resize(4, 0);
            file.read_exact_at(&mut buf, left).await?;
            let manifest_size = buf[..].as_ref().get_u32_le() as usize;
            buf.resize(manifest_size + 8, 0);
            file.read_exact_at(buf[4..].as_mut(), left).await?;

            let expected_checksum = crc32fast::hash(buf[..manifest_size + 4].as_ref());
            let actual_checksum = buf[manifest_size + 4..].as_ref().get_u32_le();
            if expected_checksum != actual_checksum {
                return Err(Corruption("manifest checksum mismatch".into()));
            }

            let recover_version = Version::decode(&buf[4..manifest_size + 4]);
            tracing::debug!("recover manifest version: {:?}", recover_version);
            versions.push(recover_version);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(ManifestFile::new(file, file_size as u64).await?)),
            },
            versions,
        ))
    }
}
