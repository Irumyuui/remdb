#![allow(unused)]

use std::{path::PathBuf, sync::Arc};

use remdb_utils::caches::{Cache, SharededLruCache};

use crate::{RemDB, error::Result, fs::IoManager, table::BlockCache};

pub struct DBOptions {
    pub(crate) memtable_size_threshold: usize,

    pub(crate) vlaue_log_size_threshold: u64,

    pub(crate) value_log_dir: PathBuf,

    pub(crate) main_db_dir: PathBuf,

    pub(crate) big_value_threshold: u32,

    pub(crate) block_size_threshold: u32,

    pub(crate) table_contains_block_count: u32,

    pub(crate) table_cache: Option<BlockCache>,

    pub(crate) io_manager: IoManager,
}

// #[derive(Debug)]
pub struct DBOpenOptions {
    memtable_size_threshold: usize,
    value_log_size_threshold: u64,
    value_log_dir: PathBuf,
    main_db_dir: PathBuf,
    big_value_threshold: u32,
    block_size_threshold: u32,
    table_contains_block_count: u32,
    table_cache: Option<BlockCache>,
}

impl Default for DBOpenOptions {
    fn default() -> Self {
        Self {
            memtable_size_threshold: 1 << 20,
            value_log_size_threshold: 1 << 30,
            value_log_dir: PathBuf::from("./vlogs"),
            main_db_dir: PathBuf::from("./remdb"),
            big_value_threshold: 4096,
            block_size_threshold: 4 << 10,
            table_contains_block_count: 100,
            table_cache: None,
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

    pub fn block_size_threshold(&mut self, bound: u32) -> &mut Self {
        self.big_value_threshold = bound;
        self
    }

    pub fn db_path(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.main_db_dir = path.into();
        self
    }

    pub fn table_contains_block_count(&mut self, count: u32) -> &mut Self {
        self.table_contains_block_count = count;
        self
    }

    pub fn enable_table_cache(&mut self) -> &mut Self {
        self.table_cache = Some(Arc::new(SharededLruCache::new(10, 10000)));
        self
    }

    pub fn build(&self) -> Result<Arc<DBOptions>> {
        let opts = DBOptions {
            memtable_size_threshold: self.memtable_size_threshold,
            vlaue_log_size_threshold: self.value_log_size_threshold,
            value_log_dir: self.value_log_dir.clone(),
            main_db_dir: self.main_db_dir.clone(),
            big_value_threshold: self.big_value_threshold,
            block_size_threshold: self.block_size_threshold,
            table_contains_block_count: self.table_contains_block_count,

            table_cache: self.table_cache.clone(),

            io_manager: IoManager::new()?,
        };
        Ok(Arc::new(opts))
    }

    pub async fn open(self) -> Result<RemDB> {
        RemDB::open(self.build()?).await
    }
}

// pub const X : usize = 1 << 20;
