#![allow(unused)]

use std::{
    collections::VecDeque,
    fmt::Debug,
    ops::Bound,
    sync::{Arc, atomic::AtomicUsize},
};

use bytes::Bytes;
use fast_async_mutex::{
    mutex::{Mutex, MutexGuard},
    rwlock::RwLock,
};

use crate::{
    error::Result,
    iterator::Iter,
    key::{self, KeySlice, Seq},
    memtable::MemTable,
    mvcc::{Mvcc, TS_END, transaction::Transaction},
    options::DBOptions,
};

// TODO: need a gc thread
#[derive(Clone)]
pub struct Core {
    mem: Arc<MemTable>,
    imms: VecDeque<Arc<MemTable>>, // old <- new
}

impl Core {
    pub fn new() -> Self {
        // TODO: recover from manifest file
        Self {
            mem: Arc::new(MemTable::new(None, 0)),
            imms: VecDeque::new(),
        }
    }
}

pub struct DBInner {
    core: Arc<RwLock<Arc<Core>>>,
    state_lock: Mutex<()>,
    pub(crate) mvcc: Mvcc,
    options: Arc<DBOptions>,

    wal_id: AtomicUsize,
}

pub enum WrireRecord<T>
where
    T: AsRef<[u8]>,
{
    Put(T, T),
    Delete(T),
}

impl<T> Debug for WrireRecord<T>
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
        let core = Arc::new(RwLock::new(Arc::new(Core::new())));

        let mvcc = Mvcc::new(0); // TODO: recover from manifest file

        let this = Self {
            core,
            state_lock: Mutex::new(()),
            mvcc,
            options,

            wal_id: AtomicUsize::new(0), // TODO: recover from manifest file
        };
        Ok(this)
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
            let value = iter.value().await;
            return if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Bytes::copy_from_slice(value)))
            };
        }
        Ok(None)
    }

    async fn once_write_with_ts(&self, key: &[u8], ts: Seq, value: &[u8]) -> Result<()> {
        let estimated_size = {
            let guard = self.core.read().await;
            guard
                .mem
                .put(KeySlice::new(key.as_ref(), ts), value.as_ref())
                .await?;
            guard.mem.memory_usage() // drop guard
        };
        self.try_freeze_current_memtable(estimated_size).await
    }

    async fn try_freeze_current_memtable(&self, estimated_size: usize) -> Result<()> {
        if estimated_size < self.options.memtable_lower_bound_size {
            return Ok(());
        }

        let freeze_state_lock = self.state_lock.lock().await; // lock in here
        let guard = self.core.read().await;
        if guard.mem.memory_usage() < self.options.memtable_lower_bound_size {
            return Ok(());
        }
        drop(guard);

        tracing::debug!(
            "try freeze current memtable, estimated_size: {}",
            estimated_size
        );

        self.force_freeze_current_memtable(&freeze_state_lock)
            .await?;

        Ok(())
    }

    async fn force_freeze_current_memtable(&self, _state_lock: &MutexGuard<'_, ()>) -> Result<()> {
        // TODO: memtable id, use wal id?
        let memtable_id = self.next_wal_id().await;
        let new_memtable = Arc::new(MemTable::new(None, memtable_id)); // TODO: create wal
        self.freeze_memtable_with_memtable(new_memtable).await?;
        // TODO: write manifest file
        Ok(())
    }

    async fn freeze_memtable_with_memtable(&self, new_memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.core.write().await;
        let mut new_core = guard.as_ref().clone();
        let old_mem = std::mem::replace(&mut new_core.mem, new_memtable);
        new_core.imms.push_back(old_mem);
        *guard = Arc::new(new_core);
        Ok(())
    }

    #[must_use]
    pub(crate) async fn write_batch_inner(
        &self,
        batch: &[WrireRecord<impl AsRef<[u8]>>],
    ) -> Result<Seq> {
        let _write_lock = self.mvcc.write_lock().await;
        let ts = self.mvcc.last_commit_ts().await + 1;

        for b in batch {
            // TODO: check flush memtable
            match b {
                WrireRecord::Put(key, value) => {
                    self.once_write_with_ts(key.as_ref(), ts, value.as_ref())
                        .await?;
                }
                WrireRecord::Delete(key) => {
                    self.once_write_with_ts(key.as_ref(), ts, &[]).await?;
                }
            }
        }

        self.mvcc.update_commit_ts(ts).await;
        Ok(ts)
    }

    pub async fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WrireRecord::Put(key, value)]).await
    }

    pub async fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        self.write_batch(&[WrireRecord::Delete(key)]).await
    }

    pub async fn write_batch(
        self: &Arc<Self>,
        batch: &[WrireRecord<impl AsRef<[u8]>>],
    ) -> Result<()> {
        let txn = self.mvcc.new_txn(self.clone()).await;
        for opt in batch {
            match opt {
                WrireRecord::Put(key, value) => txn.put(key.as_ref(), value.as_ref()).await?,
                WrireRecord::Delete(key) => txn.delete(key.as_ref()).await?,
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

    async fn next_wal_id(&self) -> usize {
        // TODO: in state lock?
        self.wal_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}
