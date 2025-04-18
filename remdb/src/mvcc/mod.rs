use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use async_channel::Sender;
use fast_async_mutex::mutex::{Mutex, MutexGuard};
use tracing::info;
use transaction::Transaction;
use watermark::Watermark;

use crate::{batch::WriteRequest, core::DBInner, format::key::Seq};

mod watermark;

pub mod transaction;

#[allow(unused)]
pub const TS_BEGIN: Seq = Seq::MAX;
#[allow(unused)]
pub const TS_END: Seq = Seq::MIN;

struct MvccVersionRecord {
    pub last_commit_ts: Seq,
    pub watermark: Watermark,
}

#[derive(Debug)]
pub(crate) struct CommitRecord {
    key_sets: HashSet<u32>,
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
