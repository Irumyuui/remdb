#![allow(unused)]

use std::sync::Arc;

use crate::{RemDB, error::Result};

pub struct DBOptions {
    pub(crate) wal_log_size: usize,
}

#[derive(Debug)]
pub struct DBOpenOptions {
    wal_log_size: usize,
}

impl Default for DBOpenOptions {
    fn default() -> Self {
        Self {
            wal_log_size: 1 << 30,
        }
    }
}

impl DBOpenOptions {
    pub fn wal_log_size(&mut self, size: usize) -> &mut Self {
        self.wal_log_size = size;
        self
    }

    pub fn build(self) -> Arc<DBOptions> {
        let opts = DBOptions {
            wal_log_size: self.wal_log_size,
        };
        Arc::new(opts)
    }

    pub async fn open(self) -> Result<RemDB> {
        RemDB::open(self.build()).await
    }
}
