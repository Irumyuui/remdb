#![allow(unused)]

use std::{path::PathBuf, sync::Arc};

use remdb_utils::caches::SharededLruCache;

use crate::{RemDB, error::KvResult, fs::IoManager, table::BlockCache};

pub struct DBOptions {
    pub(crate) memtable_size_threshold: usize,

    pub(crate) value_log_size_threshold: u64,

    pub(crate) value_log_dir: PathBuf,

    pub(crate) main_db_dir: PathBuf,

    pub(crate) big_value_threshold: u32,

    pub(crate) block_size_threshold: u32,

    pub(crate) table_contains_block_count: u32,

    pub(crate) table_cache: Option<BlockCache>,

    pub(crate) compact_tick_sec: u64,

    pub(crate) flush_tick_sec: u64,

    pub(crate) l0_limit: usize,

    pub(crate) max_levels: usize,

    pub(crate) base_level_size_mb: u64,

    pub(crate) level_size_multiplier: u64,

    pub(crate) io_manager: IoManager,
    // pub(crate) table_fd_cache: Arc<dyn Cache<u32, >>
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

    compact_tick_sec: u64,

    flush_tick_sec: u64,

    with_table_cache: bool,

    l0_limit: usize,

    max_levels: usize,

    base_level_size_mb: u64,

    level_size_multiplier: u64,
    // num_of_table_fds: usize,
}

impl Default for DBOpenOptions {
    fn default() -> Self {
        Self {
            memtable_size_threshold: 1 << 20,
            value_log_size_threshold: 1 << 30,
            value_log_dir: PathBuf::from("./t_vlogs"),
            main_db_dir: PathBuf::from("./t_remdb"),
            big_value_threshold: 4096,
            block_size_threshold: 4 << 10,
            table_contains_block_count: 100,
            with_table_cache: true,
            flush_tick_sec: 60,
            compact_tick_sec: 60,

            l0_limit: 5,
            max_levels: 12,
            base_level_size_mb: 20,
            level_size_multiplier: 20,
            // num_of_table_fds: 10000,
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

    /// sstable 中，单个 block 的大小
    pub fn block_size_threshold(&mut self, bound: u32) -> &mut Self {
        self.block_size_threshold = bound;
        self
    }

    /// db 的主要位置，包括 MANIFEST 文件、SStable 文件、WAL
    pub fn db_path(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.main_db_dir = path.into();
        self
    }

    /// 单个 sstable 中，包含的 block 数量 （暂未使用）
    pub fn table_contains_block_count(&mut self, count: u32) -> &mut Self {
        self.table_contains_block_count = count;
        self
    }

    /// 启用 table cache
    pub fn with_table_cache(&mut self, with: bool) -> &mut Self {
        self.with_table_cache = with;
        self
    }

    /// L0 的最大数量
    pub fn compact_tick_sec(&mut self, sec: u64) -> &mut Self {
        self.compact_tick_sec = sec;
        self
    }

    /// 设置多少间隔时间进行一次 memtable flush
    pub fn flush_tick_sec(&mut self, sec: u64) -> &mut Self {
        self.flush_tick_sec = sec;
        self
    }

    /// 设置 table level 的最大数量
    pub fn max_levels(&mut self, max_levels: usize) -> &mut Self {
        self.max_levels = max_levels;
        self
    }

    // /// 打开的 table 文件描述符数量
    // pub fn num_of_table_fds(&mut self, num: usize) -> &mut Self {
    //     self.num_of_table_fds = num;
    //     self
    // }

    pub fn build(&self) -> KvResult<Arc<DBOptions>> {
        let table_cache: Option<BlockCache> = if self.with_table_cache {
            Some(Arc::new(SharededLruCache::new(4, 8 * (1 << 30))))
        } else {
            None
        };

        if !self.main_db_dir.exists() {
            std::fs::create_dir_all(&self.main_db_dir)?;
        }
        if !self.value_log_dir.exists() {
            std::fs::create_dir_all(&self.value_log_dir)?;
        }

        let opts = DBOptions {
            memtable_size_threshold: self.memtable_size_threshold,
            value_log_size_threshold: self.value_log_size_threshold,
            value_log_dir: self.value_log_dir.clone(),
            main_db_dir: self.main_db_dir.clone(),
            big_value_threshold: self.big_value_threshold,
            block_size_threshold: self.block_size_threshold,
            table_contains_block_count: self.table_contains_block_count,

            compact_tick_sec: self.compact_tick_sec,
            flush_tick_sec: self.flush_tick_sec,

            table_cache,

            l0_limit: self.l0_limit,
            max_levels: self.max_levels,
            base_level_size_mb: self.base_level_size_mb,
            level_size_multiplier: self.level_size_multiplier,

            // num_of_fds: self.num_of_fds,
            io_manager: IoManager::new()?,
        };
        Ok(Arc::new(opts))
    }

    pub async fn open(&self) -> KvResult<RemDB> {
        RemDB::open(self.build()?).await
    }
}

// pub const X : usize = 1 << 20;
