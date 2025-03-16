#![allow(unused)]

use std::{path::PathBuf, sync::Arc};

use crate::{RemDB, error::Result, fs::IoManager};

pub struct DBOptions {
    pub(crate) memtable_size_threshold: usize,

    pub(crate) vlog_size_threshold: u64,

    pub(crate) vlog_dir: PathBuf,

    pub(crate) big_value_threshold: u32,

    pub(crate) io_manager: IoManager,
}

#[derive(Debug)]
pub struct DBOpenOptions {
    memtable_size_threshold: usize,

    value_log_size_threshold: u64,

    value_log_dir: PathBuf,

    big_value_threshold: u32,
}

impl Default for DBOpenOptions {
    fn default() -> Self {
        Self {
            memtable_size_threshold: 1 << 20,
            value_log_size_threshold: 1 << 30,
            value_log_dir: PathBuf::from("./vlogs"),
            big_value_threshold: 4096,
        }
    }
}

impl DBOpenOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// MemTable 的最小大小
    pub fn memtable_size_threshold(&mut self, size: usize) -> &mut Self {
        self.memtable_size_threshold = size;
        self
    }

    /// ValueLog 的最小大小
    pub fn value_log_size_threshold(&mut self, size: u64) -> &mut Self {
        self.value_log_size_threshold = size;
        self
    }

    /// ValueLog 的目录
    pub fn value_log_dir(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.value_log_dir = path.into();
        self
    }

    /// 输出到 ValueLog 值的阈值
    pub fn big_value_threshold(&mut self, bound: u32) -> &mut Self {
        self.big_value_threshold = bound;
        self
    }

    pub fn build(&self) -> Result<Arc<DBOptions>> {
        let opts = DBOptions {
            memtable_size_threshold: self.memtable_size_threshold,
            vlog_size_threshold: self.value_log_size_threshold,
            vlog_dir: self.value_log_dir.clone(),
            big_value_threshold: self.big_value_threshold,
            io_manager: IoManager::new()?,
        };
        Ok(Arc::new(opts))
    }

    pub async fn open(self) -> Result<RemDB> {
        RemDB::open(self.build()?).await
    }
}
