#![allow(unused)]

use std::{fmt::Debug, ops::Bound, sync::Arc};

use bytes::Bytes;
use fast_async_mutex::{mutex::Mutex, rwlock::RwLock};

use crate::{
    error::Result,
    iterator::Iterator,
    key::{self, KeySlice, Seq},
    memtable::MemTable,
    mvcc::{Mvcc, TS_END, transaction::Transaction},
    options::DBOptions,
};

pub struct Core {
    mem: Arc<MemTable>,
    imms: Vec<Arc<MemTable>>,
}

pub struct DBInner {
    core: Arc<RwLock<Arc<Core>>>,
    state_lock: Mutex<()>,
    pub(crate) mvcc: Mvcc,
    options: Arc<DBOptions>,
}

pub enum WriteOption<T>
where
    T: AsRef<[u8]>,
{
    Put(T, T),
    Delete(T),
}

impl<T> Debug for WriteOption<T>
where
    T: AsRef<[u8]> + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Put(arg0, arg1) => f.debug_tuple("Put").field(arg0).field(arg1).finish(),
            Self::Delete(arg0) => f.debug_tuple("Delete").field(arg0).finish(),
        }
    }
}

impl DBInner {
    pub async fn open(options: Arc<DBOptions>) -> Result<Self> {
        todo!()
    }

    pub async fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        self.mvcc.new_txn(self.clone()).await.get(key).await
    }

    pub(crate) async fn get_with_ts(&self, key: &[u8], read_ts: Seq) -> Result<Option<Bytes>> {
        let snapshot = { self.core.read().await.clone() };

        let iter = snapshot
            .mem
            .scan(
                Bound::Included(KeySlice::new(key, read_ts)),
                Bound::Included(KeySlice::new(key, TS_END)),
            )
            .await;

        if iter.is_valid().await {
            return Ok(Some(Bytes::copy_from_slice(iter.value().await)));
        }
        Ok(None)
    }

    pub(crate) async fn write_batch_inner<T>(
        &self,
        batch: &[WriteOption<impl AsRef<[u8]>>],
    ) -> Result<()> {
        let _write_lock = self.mvcc.write_lock().await;
        let ts = self.mvcc.last_commit_ts().await + 1;

        for b in batch {
            match b {
                WriteOption::Put(key, value) => {
                    let guard = self.core.read().await;
                    guard
                        .mem
                        .put(KeySlice::new(key.as_ref(), ts), value.as_ref())
                        .await?;
                }
                WriteOption::Delete(key) => {
                    let guard = self.core.read().await;
                    guard.mem.put(KeySlice::new(key.as_ref(), ts), b"").await?;
                }
            }
        }

        self.mvcc.update_commit_ts(ts).await;
        Ok(())
    }

    pub async fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteOption::Put(key, value)]).await
    }

    pub async fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteOption::Delete(key)]).await
    }

    pub async fn write_batch(
        self: &Arc<Self>,
        batch: &[WriteOption<impl AsRef<[u8]>>],
    ) -> Result<()> {
        let txn = self.mvcc.new_txn(self.clone()).await;
        for opt in batch {
            match opt {
                WriteOption::Put(key, value) => txn.put(key.as_ref(), value.as_ref()).await?,
                WriteOption::Delete(key) => txn.delete(key.as_ref()).await?,
            }
        }
        txn.commit().await?;
        Ok(())
    }

    pub async fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<()> {
        todo!()
    }

    pub async fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc.new_txn(self.clone()).await)
    }
}
