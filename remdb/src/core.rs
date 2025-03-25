#![allow(unused)]

use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize},
    },
};

use async_channel::Sender;
use bytes::Bytes;
use fast_async_mutex::{
    mutex::{Mutex, MutexGuard},
    rwlock::RwLock,
};
use itertools::Itertools;

use crate::{
    batch::{WriteEntry, WriteRequest},
    compact::level::LevelsController,
    error::{Error, Result},
    format::{
        key::{KeyBytes, KeySlice, Seq},
        value::{Value, ValuePtr},
    },
    iterator::{Iter, MergeIter},
    memtable::MemTable,
    mvcc::{Mvcc, TS_END, transaction::Transaction},
    options::DBOptions,
    table::{self, Table, table_builder::TableBuilder},
    value_log::{Entry, Request, ValueLog},
};

// TODO: need a gc thread
#[derive(Clone)]
pub struct Core {
    mem: Arc<MemTable>,
    imms: VecDeque<Arc<MemTable>>, // old <- new

    pub(crate) ssts: Vec<Vec<u32>>, // index is level, value is sst id
    pub(crate) ssts_map: HashMap<u32, Arc<Table>>,
}

impl Core {
    pub fn new(options: &DBOptions) -> Self {
        // TODO: recover from manifest file
        Self {
            mem: Arc::new(MemTable::new(None, 0)),
            imms: VecDeque::new(),

            ssts: vec![vec![]; options.max_levels],
            ssts_map: Default::default(),
        }
    }

    pub fn get_level_size(&self, level: usize) -> u64 {
        self.ssts[level]
            .iter()
            .map(|id| self.ssts_map[id].size()) // if not found table, just panic
            .sum::<u64>()
    }
}

pub struct DBInner {
    pub(crate) core: Arc<RwLock<Arc<Core>>>,
    state_lock: Mutex<()>,
    pub(crate) mvcc: Mvcc,
    next_sst_id: AtomicU32,
    vlogs: ValueLog,

    pub(crate) options: Arc<DBOptions>,
    pub(crate) levels_controller: LevelsController,
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
        let core = Arc::new(RwLock::new(Arc::new(Core::new(&options))));

        let levels = LevelsController::new(
            options.l0_limit,
            options.max_levels,
            options.base_level_size_mb,
            options.level_size_multiplier,
        );

        let mvcc = Mvcc::new(0); // TODO: recover from manifest file

        let this = Self {
            core,
            state_lock: Mutex::new(()),
            mvcc,
            next_sst_id: AtomicU32::new(1), // TODO: recover from manifest file
            vlogs: ValueLog::new(options.clone()).await?,

            options,
            levels_controller: levels,
        };
        Ok(this)
    }

    pub(crate) async fn get_with_ts(&self, key: &[u8], read_ts: Seq) -> Result<Option<Bytes>> {
        tracing::debug!("get_with_ts key: {:?} read_ts: {}", key, read_ts);

        fn check_del_value(value: Bytes) -> Option<Bytes> {
            if value.is_empty() { None } else { Some(value) }
        }

        let snapshot = { self.core.read().await.clone() };
        let key = KeySlice::new(key, read_ts);

        // value 一定在 memtable 中
        let mut memtable_iters = Vec::with_capacity(snapshot.imms.len() + 1);
        memtable_iters.push(Box::new(
            snapshot
                .mem
                .scan(
                    Bound::Included(key),
                    Bound::Included(KeySlice::new(key.key(), TS_END)),
                )
                .await,
        ));
        for imm in snapshot.imms.iter() {
            memtable_iters.push(Box::new(
                imm.scan(
                    Bound::Included(key),
                    Bound::Included(KeySlice::new(key.key(), TS_END)),
                )
                .await,
            ));
        }
        let merge_iter = MergeIter::new(memtable_iters).await;

        if merge_iter.is_valid().await {
            tracing::debug!("merge key: {:?}", merge_iter.key().await);
            let value = merge_iter.value().await;
            return Ok(check_del_value(value.value_or_ptr));
        }

        tracing::debug!("not found in memtable");
        tracing::debug!("read ts: {}, l0 sst ids: {:?}", read_ts, snapshot.ssts[0]);

        let mut l0_iters = Vec::with_capacity(snapshot.ssts[0].len());
        for l0_sst_id in snapshot.ssts[0].iter() {
            let table = snapshot.ssts_map[l0_sst_id].clone();
            tracing::debug!(
                "l0 sst id: {}, found result: {:?}",
                l0_sst_id,
                table.check_bloom_idx(key).await
            );
            if table.check_bloom_idx(key).await?.is_some() {
                l0_iters.push(Box::new(table.iter_seek_target_key(key).await?));
            }

            tracing::debug!(
                "table id: {}, block len: {}",
                table.id(),
                table.block_count()
            );

            let mut iter = table.iter().await?;
            while iter.is_valid().await {
                tracing::debug!(
                    "table id : {}, key: {:?}, value: {:?}",
                    table.id(),
                    iter.key().await,
                    iter.value().await
                );
                iter.next().await?;
            }
        }
        let l0_iter = MergeIter::new(l0_iters).await;
        if l0_iter.is_valid().await {
            tracing::debug!("l0 key: {:?}", l0_iter.key().await);
            let value = l0_iter.value().await;
            if value.meta.is_value() {
                return Ok(check_del_value(value.value_or_ptr));
            }

            let res = self
                .vlogs
                .read_entry(ValuePtr::decode(&value.value_or_ptr)?)
                .await?;

            assert_eq!(KeySlice::new(&res.key, res.header.seq), key);
            return Ok(check_del_value(res.value));
        }

        Ok(None)
    }

    async fn write_once(&self, key: KeyBytes, value: Bytes) -> Result<()> {
        let estimated_size = {
            let guard = self.core.read().await;
            guard.mem.put(key, value).await?;
            guard.mem.memory_usage() // drop guard
        };
        self.try_freeze_current_memtable(estimated_size).await
    }

    async fn try_freeze_current_memtable(&self, estimated_size: usize) -> Result<()> {
        if estimated_size < self.options.memtable_size_threshold {
            return Ok(());
        }

        let freeze_state_lock = self.state_lock.lock().await; // lock in here
        let guard = self.core.read().await;
        if guard.mem.memory_usage() < self.options.memtable_size_threshold {
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

    pub(crate) async fn force_freeze_current_memtable_for_test(&self) {
        let state_lock = self.state_lock.lock().await;
        self.force_freeze_current_memtable(&state_lock)
            .await
            .unwrap();
    }

    async fn force_freeze_current_memtable(&self, _state_lock: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id().await;
        let new_memtable = Arc::new(MemTable::new(None, memtable_id)); // TODO: create wal
        self.freeze_memtable_with_memtable(new_memtable).await?;
        // TODO: write manifest file

        tracing::debug!("freeze current memtable, id: {}", memtable_id);
        Ok(())
    }

    async fn freeze_memtable_with_memtable(&self, new_memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.core.write().await;
        let mut new_core = guard.as_ref().clone();
        let old_mem = std::mem::replace(&mut new_core.mem, new_memtable);
        new_core.imms.push_back(old_mem);

        tracing::debug!(
            "freeze memtable, current status, mem: {}, imms: {:?}",
            new_core.mem.id(),
            new_core.imms.iter().map(|m| m.id()).collect_vec()
        );

        *guard = Arc::new(new_core);
        Ok(())
    }

    pub(crate) async fn write_batch_inner(&self, entries: &[WriteEntry]) -> Result<()> {
        for e in entries {
            self.write_once(e.key.clone(), e.value.clone()).await?;
        }
        Ok(())
    }

    pub async fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<()> {
        todo!()
    }

    pub async fn new_txn(
        self: &Arc<Self>,
        write_sender: Sender<WriteRequest>,
    ) -> Result<Arc<Transaction>> {
        Ok(self.mvcc.new_txn(self.clone(), write_sender).await)
    }

    /// memtable id is same as sst id
    async fn next_sst_id(&self) -> u32 {
        // TODO: in state lock?
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

/* #region Flush memtable */

impl DBInner {
    async fn should_flush_immutable_memtable(&self) -> bool {
        !self.core.read().await.imms.is_empty()
    }

    pub async fn try_flush_immutable_memtable(&self) -> Result<bool> {
        if self.should_flush_immutable_memtable().await {
            self.force_flush_immutable_memtable().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    // TODO: compact task
    pub async fn force_flush_immutable_memtable(&self) -> Result<()> {
        tracing::debug!("force_flush_immutable_memtable");

        let _flush_lock = self.state_lock.lock().await;

        let await_flush_memtable = {
            match self.core.read().await.imms.front() {
                Some(imms) => imms.clone(),
                None => {
                    // should not happen
                    return Err(Error::MemTableFlush(
                        "immutable memtable list is empty".into(),
                    ));
                }
            }
        };

        let mut builder = TableBuilder::new(self.options.clone());
        flush_immutable_memtable(
            &await_flush_memtable,
            &mut builder,
            &self.vlogs,
            &self.options,
        )
        .await?;

        // finish build
        let sst_id = await_flush_memtable.id();
        let table = Arc::new(builder.finish(sst_id).await?);

        self.append_level0_table(table, &_flush_lock).await?;

        Ok(())
    }

    async fn append_level0_table(
        &self,
        table: Arc<Table>,
        _flush_lock: &MutexGuard<'_, ()>,
    ) -> Result<()> {
        let mut guard = self.core.write().await;
        let mut snapshot = guard.as_ref().clone();

        let deleted_memtable = snapshot.imms.pop_front().expect("imms is not empty");
        assert_eq!(deleted_memtable.id(), table.id());

        snapshot.ssts[0].push(table.id());

        tracing::info!("imm memtbale flush to level0 sst, id: {}", table.id());
        let res = snapshot.ssts_map.insert(table.id(), table);
        assert!(res.is_none());

        // TODO: garbage collection
        let _prev_version = std::mem::replace(&mut *guard, Arc::new(snapshot));

        // TODO: manifest file add version
        // TODO: shoud finish manifest ahaed, then do gc

        Ok(())
    }
}

async fn flush_immutable_memtable(
    memtable: &Arc<MemTable>,
    builder: &mut TableBuilder,
    vlogs: &ValueLog,
    options: &DBOptions,
) -> Result<()> {
    let mut iter = memtable.iter();

    tracing::debug!("flush imm memtable, id: {}", memtable.id());

    while iter.is_valid().await {
        let key = iter.raw_key();
        let raw_value = iter.raw_value();

        let value = if raw_value.len() >= options.big_value_threshold as usize {
            // TODO: use inplace_vec ?
            let req = Request::new(vec![Entry::new(
                key.seq(),
                key.real_key.clone(),
                raw_value.clone(),
            )]);
            let mut reqs = [req];
            vlogs.write_requests(&mut reqs).await?;
            Value::from_ptr(&reqs[0].value_ptrs[0])
        } else {
            Value::from_raw_value(raw_value)
        };

        tracing::debug!("flush key: {:?}, value: {:?}", key, value);
        builder.add(key, value);

        iter.next().await?;
    }

    Ok(())
}

/* #endregion Flush memtable */
