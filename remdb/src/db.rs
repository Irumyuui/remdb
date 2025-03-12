#![allow(unused)]

use std::{ops::Bound, sync::Arc};

use bytes::Bytes;
use fast_async_mutex::mutex::Mutex;

use crate::{error::Result, memtable::MemTable, mvcc::Mvcc, options::DBOptions};

pub struct Core {
    mem: Arc<MemTable>,
    imms: Vec<Arc<MemTable>>,
}

pub struct DBInner {
    core: Arc<Core>,
    state_lock: Mutex<()>,
    mvcc: Mvcc,
    options: Arc<DBOptions>,
}

pub enum WriteBatch<T: AsRef<[u8]>> {
    Put(T, T),
    Delete(T),
}

impl DBInner {
    pub async fn get(&self, key: &[u8]) -> Result<Bytes> {
        todo!()
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    pub async fn write_batch(&self) -> Result<()> {
        todo!()
    }

    pub async fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<()> {
        todo!()
    }
}
