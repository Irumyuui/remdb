#![allow(unused)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, atomic::AtomicBool},
};

use fast_async_mutex::mutex::{Mutex, MutexGuard};
use transaction::Transaction;
use watermark::Watermark;

use crate::{db::DBInner, key::Seq};

pub mod transaction;
pub mod watermark;

pub const TS_BEGIN: Seq = Seq::MAX;
pub const TS_END: Seq = Seq::MIN;

struct MvccVersionRecord {
    pub last_commit_ts: Seq,
    pub watermark: Watermark,
}

pub struct CommitRecord {
    key_sets: BTreeSet<u32>,
}

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

    pub async fn last_commit_ts(&self) -> Seq {
        self.ts.lock().await.last_commit_ts
    }

    pub async fn update_commit_ts(&self, ts: Seq) {
        self.ts.lock().await.last_commit_ts = ts;
    }

    pub async fn watermark(&self) -> Seq {
        self.ts.lock().await.watermark.watermark().unwrap_or(0)
    }

    pub async fn new_txn(&self, db: Arc<DBInner>) -> Arc<Transaction> {
        todo!()
    }

    pub async fn write_lock(&self) -> MutexGuard<'_, ()> {
        self.write_lock.lock().await
    }
}
