#![allow(unused)]

use std::{
    collections::{BTreeMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use fast_async_mutex::{mutex::Mutex, rwlock::RwLock};
use tracing::info;

use crate::{
    core::DBInner,
    error::{Error, Result},
    key::Seq,
};

struct OperatorRecoder {
    read: HashSet<u32>,
    write: HashSet<u32>,
}

pub struct Transaction {
    read_ts: Seq,
    db: Arc<DBInner>,

    txn_data: Arc<RwLock<BTreeMap<Bytes, Bytes>>>,
    commited: Arc<AtomicBool>,

    operator_recorder: Option<Mutex<OperatorRecoder>>, // read, write
}

impl Transaction {
    pub fn check_commit(&self) -> Result<()> {
        if self.commited() {
            Err(Error::Txn("txn has been commited".into()))
        } else {
            Ok(())
        }
    }

    fn commited(&self) -> bool {
        self.commited.load(Ordering::Relaxed)
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.check_commit()?;

        if let Some(ref operator_recorder) = self.operator_recorder {
            let mut guard = operator_recorder.lock().await;
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            let key_hash = hasher.finish() as u32;
            guard.read.insert(key_hash);
        }

        if let Some(e) = self.txn_data.read().await.get(key) {
            if e.is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(e.clone()));
            }
        }

        self.db.get_with_ts(key, self.read_ts).await
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_commit()?;

        if let Some(ref operator_recorder) = self.operator_recorder {
            let mut guard = operator_recorder.lock().await;
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            let key_hash = hasher.finish() as u32;
            guard.write.insert(key_hash);
        }
        self.txn_data
            .write()
            .await
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));

        Ok(())
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[]).await
    }

    pub async fn commit(&self) -> Result<()> {
        self.commited
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| Error::Txn("txn has been commited".into()))?;

        let _commit_lock = self.db.mvcc.commit_lock().await;

        if let Some(ref recoder) = self.operator_recorder {
            let guard = recoder.lock().await;
            info!(
                "txn ready to commit, read: {:?}, write: {:?}",
                guard.read, guard.write
            );

            if !guard.write.is_empty() {
                // check read ts if the key has been modified
                let commit_info = self.db.mvcc.commited_txns.lock().await;
                for (_, txn_keys) in commit_info.range(self.read_ts + 1..) {
                    for hs in &guard.read {
                        if txn_keys.key_sets.contains(hs) {
                            return Err(Error::Txn("key has been modified".into()));
                        }
                    }
                }
            }
        }

        todo!()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        todo!()
    }
}
