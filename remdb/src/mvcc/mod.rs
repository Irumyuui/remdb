#![allow(unused)]

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::{Arc, atomic::AtomicBool},
};

use async_channel::Sender;
use fast_async_mutex::mutex::{Mutex, MutexGuard};
use tracing::info;
use transaction::Transaction;
use watermark::Watermark;

use crate::{batch::WriteRequest, core::DBInner, format::key::Seq};

mod watermark;

pub mod transaction;

pub const TS_BEGIN: Seq = Seq::MAX;
pub const TS_END: Seq = Seq::MIN;

struct MvccVersionRecord {
    pub last_commit_ts: Seq,
    pub watermark: Watermark,
}

#[derive(Debug)]
pub(crate) struct CommitRecord {
    key_sets: HashSet<u32>,
}

/// 基于 Optimistic Concurrency Control 的事务
///
/// 每个事务都会有一个本地存储的读写集合，写的所有数据先保存在本地存储之中，而读数据从本地和实际数据中读取。当事务需要提交时，需要将所提交的事务数据与其他事务提交的数据进行对比，如果食物出现了冲突，那么需要 abort ，一般情况下就是提示用户数据 commit failed ，此时可以像乐观锁一样重试。
///
/// 每一个事务分为三个阶段：
///
/// 1. 读阶段：对于数据库本身，数据库的数据是只读的，事务只能读取这部分只读数据，其他数据需要保存在自身本地存储上。
///
/// 2. 校验阶段：事务提交时，需要校验与其他事务中的数据，一般来说失败的情况比较少见，如果失败了，那么需要重试。
///
/// 3. 写阶段：事务提交时，需要将本地存储的数据写入数据库中，这个过程是原子的，因为检查过了一般来说没有失败，除非位于同一个时间戳的事务被错误提交了。
pub struct Mvcc {
    write_lock: Mutex<()>,
    commit_lock: Mutex<()>,
    ts: Arc<Mutex<MvccVersionRecord>>,
    commited_txns: Arc<Mutex<BTreeMap<Seq, CommitRecord>>>,
}

impl Mvcc {
    pub fn new(init_ts: Seq) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new(MvccVersionRecord {
                last_commit_ts: init_ts,
                watermark: Watermark::new(),
            })),
            commited_txns: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn get_commit_ts(&self, txn: &Transaction) -> Seq {
        todo!()
    }

    pub async fn last_commit_ts(&self) -> Seq {
        self.ts.lock().await.last_commit_ts
    }

    pub async fn update_commit_ts(&self, ts: Seq) {
        self.ts.lock().await.last_commit_ts = ts;
    }

    pub async fn watermark(&self) -> Seq {
        self.ts.lock().await.watermark.watermark().unwrap_or(0)
    }

    pub async fn new_txn(
        &self,
        db: Arc<DBInner>,
        write_sender: Sender<WriteRequest>,
    ) -> Arc<Transaction> {
        let txn = {
            let mut version_record = self.ts.lock().await;
            let read_ts = version_record.last_commit_ts;
            version_record.watermark.add_reader(read_ts);
            Arc::new(Transaction::new(read_ts, db, write_sender))
        };
        info!("new txn, read_ts: {}", txn.read_ts());
        txn
    }

    pub async fn write_lock(&self) -> MutexGuard<'_, ()> {
        self.write_lock.lock().await
    }

    async fn commit_lock(&self) -> MutexGuard<'_, ()> {
        self.commit_lock.lock().await
    }
}
