#![allow(unused)]

use std::sync::Arc;

use fast_async_mutex::mutex::Mutex;
use transaction::Transaction;
use watermark::Watermark;

use crate::key::Seq;

pub mod transaction;
pub mod watermark;

struct MvccVersionRecord {
    pub last_commit_ts: Seq,
    pub watermark: Watermark,
}

pub struct Mvcc {
    write_lock: Mutex<()>,
    commit_lock: Mutex<()>,
    ts: Arc<Mutex<MvccVersionRecord>>,
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

    pub async fn new_txn(&self) -> Arc<Transaction> {
        todo!()
    }
}
