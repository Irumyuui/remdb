use std::sync::Arc;

use crate::{RemDB, error::Result};

pub struct Options {
    pub(crate) memtable_size: usize,
}

impl Options {
    pub fn new() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024,
        }
    }

    pub fn memtable_size(&mut self, memtable_size: usize) -> &mut Self {
        self.memtable_size = memtable_size;
        self
    }

    pub fn build(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn open(self) -> Result<RemDB> {
        RemDB::open(self.build())
    }
}
