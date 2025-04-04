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
use futures::StreamExt;
use itertools::Itertools;

use crate::{
    batch::{WriteEntry, WriteRequest},
    compact::level::LevelsController,
    error::{KvError, KvResult},
    format::{
        key::{KeyBytes, KeySlice, Seq},
        value::{Value, ValuePtr},
    },
    kv_iter::prelude::*,
    memtable::MemTable,
    mvcc::{Mvcc, TS_END, transaction::Transaction},
    options::DBOptions,
    table::{self, Table, table_builder::TableBuilder, table_iter::TableConcatIter},
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
    pub(crate) state_lock: Mutex<()>,
    pub(crate) mvcc: Mvcc,
    next_table_id: AtomicU32,
    pub(crate) vlogs: Arc<ValueLog>,

    pub(crate) options: Arc<DBOptions>,
    pub(crate) levels_controller: LevelsController,
}

impl DBInner {
    pub async fn open(options: Arc<DBOptions>) -> KvResult<Self> {
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
            next_table_id: AtomicU32::new(1), // TODO: recover from manifest file
            vlogs: ValueLog::new(options.clone()).await?.into(),

            options,
            levels_controller: levels,
        };
        Ok(this)
    }

    pub(crate) async fn get_with_ts(&self, key: &[u8], read_ts: Seq) -> KvResult<Option<Bytes>> {
        tracing::debug!("get_with_ts key: {:?} read_ts: {}", key, read_ts);

        fn check_del_value(value: Bytes) -> Option<Bytes> {
            if value.is_empty() { None } else { Some(value) }
        }

        let snapshot = { self.core.read().await.clone() };
        let key = KeySlice::new(key, read_ts);

        // value 一定在 memtable 中
        let mut memtable_iters = Vec::with_capacity(snapshot.imms.len() + 1);
        memtable_iters.push(
            snapshot
                .mem
                .scan(
                    Bound::Included(key),
                    Bound::Included(KeySlice::new(key.key(), TS_END)),
                )
                .await,
        );
        for imm in snapshot.imms.iter() {
            memtable_iters.push(
                imm.scan(
                    Bound::Included(key),
                    Bound::Included(KeySlice::new(key.key(), TS_END)),
                )
                .await,
            );
        }
        let mut merge_iter = MergeIter::new(memtable_iters).await?;

        if let Some(item) = merge_iter.next().await? {
            tracing::debug!("merge key: {:?}", item.key);
            let value = item.value;
            return Ok(check_del_value(value.as_raw_value().clone()));
        }

        tracing::debug!("not found in memtable");
        tracing::debug!("read ts: {}, l0 sst ids: {:?}", read_ts, snapshot.ssts[0]);

        let searth_to_vlog = async |item: Option<KvItem>| -> KvResult<Option<Bytes>> {
            if let Some(item) = item {
                let value = item.value;
                if value.is_raw_value() {
                    return Ok(check_del_value(value.as_raw_value().clone()));
                }
                let res = self.vlogs.read_entry(*value.as_value_ptr()).await?;
                assert_eq!(KeySlice::new(&res.key, res.header.seq), key);
                return Ok(check_del_value(res.value));
            }
            Ok(None)
        };

        let mut l0_iters = Vec::with_capacity(snapshot.ssts[0].len());
        for l0_sst_id in snapshot.ssts[0].iter() {
            let table = snapshot.ssts_map[l0_sst_id].clone();
            tracing::debug!(
                "l0 sst id: {}, found result: {:?}",
                l0_sst_id,
                table.check_bloom_idx(key).await
            );
            if table.check_bloom_idx(key).await?.is_some() {
                l0_iters.push(table.iter_seek_target_key(key).await?);
            }
        }
        let mut l0_iter = MergeIter::new(l0_iters).await?;

        let mut level_iter = vec![];
        for ids in snapshot.ssts.iter().skip(1) {
            let tables = ids
                .iter()
                .map(|id| snapshot.ssts_map[id].clone())
                .collect_vec();
            let mut concat_iter = TableConcatIter::new(tables);
            concat_iter.seek_to_key(key).await?;
            level_iter.push(concat_iter);
        }
        let level_merge_iter = MergeIter::new(level_iter).await?;
        let mut two_merge_iter = TwoMergeIter::new(l0_iter, level_merge_iter).await?;

        searth_to_vlog(two_merge_iter.next().await?).await
    }

    async fn write_once(&self, key: KeyBytes, value: Bytes) -> KvResult<()> {
        let estimated_size = {
            let guard = self.core.read().await;
            guard.mem.put(key, value).await?;
            guard.mem.memory_usage() // drop guard
        };
        self.try_freeze_current_memtable(estimated_size).await
    }

    async fn try_freeze_current_memtable(&self, estimated_size: usize) -> KvResult<()> {
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

    async fn force_freeze_current_memtable(
        &self,
        _state_lock: &MutexGuard<'_, ()>,
    ) -> KvResult<()> {
        let memtable_id = self.next_table_id().await;
        let new_memtable = Arc::new(MemTable::new(None, memtable_id)); // TODO: create wal
        self.freeze_memtable_with_memtable(new_memtable).await?;
        // TODO: write manifest file

        tracing::debug!("freeze current memtable, id: {}", memtable_id);
        Ok(())
    }

    async fn freeze_memtable_with_memtable(&self, new_memtable: Arc<MemTable>) -> KvResult<()> {
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

    // should be called in one thread.
    pub(crate) async fn write_batch_inner(&self, entries: &[WriteEntry]) -> KvResult<()> {
        for e in entries {
            self.write_once(e.key.clone(), e.value.clone()).await?;
        }
        Ok(())
    }

    pub async fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> KvResult<()> {
        todo!()
    }

    pub async fn new_txn(
        self: &Arc<Self>,
        write_sender: Sender<WriteRequest>,
    ) -> KvResult<Arc<Transaction>> {
        Ok(self.mvcc.new_txn(self.clone(), write_sender).await)
    }

    /// memtable id is same as sst id
    pub(crate) async fn next_table_id(&self) -> u32 {
        // TODO: in state lock?
        self.next_table_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

/* #region Flush memtable */

impl DBInner {
    async fn should_flush_immutable_memtable(&self) -> bool {
        !self.core.read().await.imms.is_empty()
    }

    pub async fn try_flush_immutable_memtable(&self) -> KvResult<bool> {
        if self.should_flush_immutable_memtable().await {
            self.force_flush_immutable_memtable().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn force_flush_immutable_memtable(&self) -> KvResult<()> {
        tracing::debug!("force_flush_immutable_memtable");

        let _flush_lock = self.state_lock.lock().await;

        let await_flush_memtable = {
            match self.core.read().await.imms.front() {
                Some(imms) => imms.clone(),
                None => return Err(KvError::ImmutableMemtableNotFound),
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
    ) -> KvResult<()> {
        let mut guard = self.core.write().await;
        let mut snapshot = guard.as_ref().clone();

        let deleted_memtable = snapshot.imms.pop_front().expect("imms is not empty");
        assert_eq!(deleted_memtable.id(), table.id());

        snapshot.ssts[0].push(table.id());

        tracing::info!("imm memtbale flush to level0 sst, id: {}", table.id());
        let res = snapshot.ssts_map.insert(table.id(), table);
        assert!(res.is_none());

        let _prev_version = std::mem::replace(&mut *guard, Arc::new(snapshot));

        // TODO: manifest file add version
        Ok(())
    }

    pub async fn do_vlog_gc(
        &self,
        write_req_sender: Sender<WriteRequest>,
    ) -> KvResult<Option<u32>> {
        self.vlogs.do_gc(self, write_req_sender).await
    }
}

async fn flush_immutable_memtable(
    memtable: &Arc<MemTable>,
    builder: &mut TableBuilder,
    vlogs: &ValueLog,
    options: &DBOptions,
) -> KvResult<()> {
    tracing::debug!("flush imm memtable, id: {}", memtable.id());

    let mut iter = memtable.iter();
    while let Some(item) = iter.next().await? {
        let key = item.key;
        let raw_value = item.value.as_raw_value().clone();
        let value = if raw_value.len() >= options.big_value_threshold as usize {
            // TODO: use inplace_vec ?
            let req = Request::new(vec![Entry::new(
                key.seq(),
                key.real_key.clone(),
                raw_value.clone(),
            )]);
            let mut reqs = [req];
            vlogs.write_requests(&mut reqs).await?;
            Value::from_ptr(reqs[0].value_ptrs[0])
        } else {
            Value::from_raw_value(raw_value)
        };
        builder.add(key, value);
    }

    Ok(())
}

/* #endregion Flush memtable */
