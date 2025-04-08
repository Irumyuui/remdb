use std::{
    collections::{BTreeMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_channel::Sender;
use bytes::Bytes;
use fast_async_mutex::{
    mutex::{Mutex, MutexGuard},
    rwlock::RwLock,
};

use crate::{
    batch::{WriteEntry, WriteRequest},
    core::DBInner,
    error::{KvError, KvResult, NoFail},
    format::key::{KeyBytes, KeySlice, Seq},
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

    write_sender: Sender<WriteRequest>,

    operator_recorder: Option<Mutex<OperatorRecoder>>, // read, write
}

impl Transaction {
    pub fn new(read_ts: Seq, db_inner: Arc<DBInner>, write_sender: Sender<WriteRequest>) -> Self {
        Self {
            read_ts,
            db_inner,

            txn_data: Arc::new(RwLock::new(BTreeMap::new())),
            commited: Arc::new(AtomicBool::new(false)),

            write_sender,

            operator_recorder: Some(Mutex::new(OperatorRecoder {
                read: HashSet::default(),
                write: HashSet::default(),
            })),
        }
    }

    pub fn check_commit(&self) -> KvResult<()> {
        match self.is_commited() {
            true => Err(KvError::TxnCommited(self.read_ts)),
            false => Ok(()),
        }
    }

    fn is_commited(&self) -> bool {
        self.commited.load(Ordering::Relaxed)
    }

    pub async fn get(&self, key: &[u8]) -> KvResult<Option<Bytes>> {
        self.check_commit()?;

        if let Some(ref operator_recorder) = self.operator_recorder {
            let mut guard = operator_recorder.lock().await;
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            let key_hash = hasher.finish() as u32;
            guard.read.insert(key_hash);
        }

        if let Some(e) = self.txn_data.read().await.get(key) {
            match e.is_empty() {
                true => return Ok(None),
                false => return Ok(Some(e.clone())),
            }
        }

        self.db_inner
            .get_inner(KeySlice::new(key, self.read_ts).into_key_bytes())
            .await
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> KvResult<()> {
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

    pub async fn delete(&self, key: &[u8]) -> KvResult<()> {
        self.put(key, &[]).await
    }

    pub async fn scan(&self, _lower: Bound<&[u8]>, _upper: Bound<&[u8]>) {
        todo!()
    }

    /// Check if readed keys has been modified by forward txns. \
    /// If not write any key, that means just a readonly transaction, just do nothing for check.
    async fn serializable_check(&self, _commit_lock: &MutexGuard<'_, ()>) -> KvResult<()> {
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
                            // return Err(KvError::Txn("key has been modified".into()));
                            return Err(KvError::TxnSerializableFailed {
                                current_ts: self.read_ts,
                                forward_ts: *ts,
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Commit and send write request to write task.
    async fn commit_and_send(&self) -> KvResult<u64> {
        let _write_lock = self.db_inner.mvcc.write_lock().await;
        let commit_ts = self.db_inner.mvcc.last_commit_ts().await + 1;

        let local_data = self.txn_data.read().await;
        let mut entries_with_ts = Vec::with_capacity(local_data.len());
        for (k, v) in local_data.iter() {
            entries_with_ts.push(WriteEntry {
                key: KeyBytes::new(k.clone(), commit_ts),
                value: v.clone(),
            });
        }

        let (s, t) = async_channel::bounded(1);
        let req = WriteRequest::Batch {
            entries: entries_with_ts,
            result_sender: s,
        };
        self.write_sender.send(req).await.unwrap();

        // TODO: jump
        t.recv().await.unwrap().to_no_fail();

        self.db_inner.mvcc.update_commit_ts(commit_ts).await;
        Ok(commit_ts)
    }

    // TODO: use `self` instead of `&self`?
    pub async fn commit(&self) -> KvResult<()> {
        self.commited
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| KvError::TxnCommited(self.read_ts))?;

        let _commit_lock = self.db_inner.mvcc.commit_lock().await;
        self.serializable_check(&_commit_lock).await?;

        let comited_ts = self.commit_and_send().await?;
        tracing::debug!("txn commit, res_ts: {}", comited_ts);

        let mut committed_txns = self.db_inner.mvcc.commited_txns.lock().await;
        let mut opt_list = self.operator_recorder.as_ref().unwrap().lock().await;

        // insert write set to enable serializable check
        let old_write_set = std::mem::take(&mut opt_list.write);
        let old_data = committed_txns.insert(
            comited_ts,
            CommitRecord {
                key_sets: old_write_set,
            },
        );
        assert!(old_data.is_none());

        // too many write set, should remove olds
        let watermark = self.db_inner.mvcc.watermark().await;
        while let Some(entry) = committed_txns.first_entry() {
            if *entry.key() <= watermark {
                entry.remove();
            } else {
                break;
            }
        }

        tracing::info!("txn commit, read_ts: {}", self.read_ts);

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
            tracing::info!("txn drop, read_ts: {}", self.read_ts);
        })
    }
}
