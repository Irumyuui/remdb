#![allow(unused)]

use std::{path::PathBuf, sync::Arc};

use crate::{RemDB, error::Result, fs::IoManager};

pub struct DBOptions {
    pub(crate) vlog_size: usize,

    pub(crate) vlog_dir_path: PathBuf,

    pub(crate) big_value_lower_bound: u32,

    pub(crate) io_manager: IoManager,
}

#[derive(Debug)]
pub struct DBOpenOptions {
    vlog_size: usize,

    vlog_dir_path: PathBuf,

    big_value_lower_bound: u32,
}

impl Default for DBOpenOptions {
    fn default() -> Self {
        Self {
            vlog_size: 1 << 30,
            vlog_dir_path: PathBuf::from("./vlogs"),
            big_value_lower_bound: 4096,
        }
    }
}

impl DBOpenOptions {
    pub fn vlog_size(&mut self, size: usize) -> &mut Self {
        self.vlog_size = size;
        self
    }

    pub fn vlog_dir_path(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.vlog_dir_path = path.into();
        self
    }

    pub fn big_value_lower_bound(&mut self, bound: u32) -> &mut Self {
        self.big_value_lower_bound = bound;
        self
    }

    pub fn build(self) -> Result<Arc<DBOptions>> {
        let opts = DBOptions {
            vlog_size: self.vlog_size,
            vlog_dir_path: self.vlog_dir_path,
            big_value_lower_bound: self.big_value_lower_bound,

            io_manager: IoManager::new()?,
        };
        Ok(Arc::new(opts))
    }

    pub async fn open(self) -> Result<RemDB> {
        RemDB::open(self.build()?).await
    }
}
