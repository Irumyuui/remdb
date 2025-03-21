#![allow(unused)]

use std::{
    collections::{BTreeMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use fast_async_mutex::{mutex::Mutex, rwlock::RwLock};
use itertools::Itertools;
use tracing::info;

use crate::{
    core::{DBInner, WrireRecord},
    error::{Error, Result},
    format::key::Seq,
    mvcc::CommitRecord,
};

struct OperatorRecoder {
    read: HashSet<u32>,
    write: HashSet<u32>,
}

pub struct Transaction {
    read_ts: Seq,
    db_inner: Arc<DBInner>,

    txn_data: Arc<RwLock<BTreeMap<Bytes, Bytes>>>, // TODO: use lockfree container
    commited: Arc<AtomicBool>,

    operator_recorder: Option<Mutex<OperatorRecoder>>, // read, write
}

impl Transaction {
    pub fn new(read_ts: Seq, db_inner: Arc<DBInner>) -> Self {
        Self {
            read_ts,
            db_inner,

            txn_data: Arc::new(RwLock::new(BTreeMap::new())),
            commited: Arc::new(AtomicBool::new(false)),

            operator_recorder: Some(Mutex::new(OperatorRecoder {
                read: HashSet::default(),
                write: HashSet::default(),
            })),
        }
    }

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

        self.db_inner.get_with_ts(key, self.read_ts).await
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

    pub async fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) {
        todo!()
    }

    // TODO: use `self` instead of `&self`?
    pub async fn commit(&self) -> Result<()> {
        self.commited
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| Error::Txn("txn has been commited".into()))?;

        let _commit_lock = self.db_inner.mvcc.commit_lock().await;

        if let Some(ref recoder) = self.operator_recorder {
            let guard = recoder.lock().await;
            tracing::debug!(
                "txn ready to commit, read: {:?}, write: {:?}",
                guard.read,
                guard.write
            );

            if !guard.write.is_empty() {
                // check read ts if the key has been modified
                let commit_info = self.db_inner.mvcc.commited_txns.lock().await;

                for (ts, txn_keys) in commit_info.range(self.read_ts + 1..) {
                    tracing::debug!("later ts: {}, txn_keys: {:?}", ts, txn_keys.key_sets);
                    for hs in &guard.read {
                        if txn_keys.key_sets.contains(hs) {
                            return Err(Error::Txn("key has been modified".into()));
                        }
                    }
                }
            }
        }

        let batch = self
            .txn_data
            .read()
            .await
            .iter()
            .map(|(k, v)| {
                if v.is_empty() {
                    WrireRecord::Delete(k.clone())
                } else {
                    WrireRecord::Put(k.clone(), v.clone())
                }
            })
            .collect_vec();
        let res_ts = self.db_inner.write_batch_inner(&batch[..]).await?;

        tracing::debug!("txn commit, res_ts: {}", res_ts);

        // check serializable
        let mut committed_txns = self.db_inner.mvcc.commited_txns.lock().await;
        let mut opt_list = self.operator_recorder.as_ref().unwrap().lock().await;

        let old_write_set = std::mem::take(&mut opt_list.write);

        let old_data = committed_txns.insert(
            res_ts,
            CommitRecord {
                key_sets: old_write_set,
            },
        );
        assert!(old_data.is_none());

        let watermark = self.db_inner.mvcc.watermark().await;
        while let Some(entry) = committed_txns.first_entry() {
            if *entry.key() <= watermark {
                entry.remove();
            } else {
                break;
            }
        }

        info!("txn commit, read_ts: {}", self.read_ts);

        Ok(())
    }

    pub(crate) fn read_ts(&self) -> Seq {
        self.read_ts
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        futures::executor::block_on(async {
            self.db_inner
                .mvcc
                .ts
                .lock()
                .await
                .watermark
                .remove_reader(self.read_ts);
            info!("txn drop, read_ts: {}", self.read_ts);
        })
    }
}
