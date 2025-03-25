use std::sync::Arc;

use async_channel::Sender;
use bytes::Bytes;
use tokio::task::JoinHandle;
use tracing::info;

use crate::{
    core::{DBInner, WrireRecord},
    error::{Result, no_fail},
    format::{key::KeyBytes, lock_db, unlock_db},
    mvcc::transaction::Transaction,
    options::DBOptions,
};

pub struct RemDB {
    inner: Arc<DBInner>,
    _options: Arc<DBOptions>,

    write_task: Option<JoinHandle<()>>,
    write_batch_sender: Sender<WriteRequest>,

    flush_task: Option<JoinHandle<()>>,
    flush_closed_sender: Sender<()>,
}

#[derive(Debug)]
pub struct WriteEntry {
    pub(crate) key: KeyBytes,
    pub(crate) value: Bytes,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum WriteRequest {
    Batch {
        entries: Vec<WriteEntry>,
        result_sender: Sender<Result<()>>,
    },
    Exit,
}

impl RemDB {
    pub async fn open(options: Arc<DBOptions>) -> Result<Self> {
        info!("RemDB begin to open");

        lock_db(&options.main_db_dir, &options.value_log_dir).await?;

        let inner = Arc::new(DBInner::open(options.clone()).await?);
        let (write_batch_sender, write_batch_receiver) = async_channel::unbounded();
        let write_task = inner
            .register_write_batch_task(write_batch_receiver)
            .await?;
        let (flush_closed_sender, flush_closed_receiver) = async_channel::bounded(1);
        let flush_task = inner.register_flush_task(flush_closed_receiver).await?;

        let this = Self {
            inner: inner.clone(),
            write_batch_sender,
            flush_closed_sender,
            _options: options,
            write_task: Some(write_task),
            flush_task: Some(flush_task),
        };

        info!("RemDB opened");

        Ok(this)
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.begin_transaction().await?.get(key).await
    }

    pub async fn scan(&self) {
        // need iter
        todo!()
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let txn = self.begin_transaction().await?;
        txn.put(key, value).await?;
        txn.commit().await?;
        Ok(())
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let txn = self.begin_transaction().await?;
        txn.put(key, &[]).await?;
        txn.commit().await?;
        Ok(())
    }

    pub async fn write_batch(&self, data: &[WrireRecord<&[u8]>]) -> Result<()> {
        let txn = self.begin_transaction().await?;
        for b in data.iter() {
            match b {
                WrireRecord::Put(key, value) => txn.put(key, value).await?,
                WrireRecord::Delete(key) => txn.delete(key).await?,
            }
        }
        txn.commit().await?;
        Ok(())
    }

    async fn send_write_request(&self, req: WriteRequest) -> Result<()> {
        if let Err(e) = self.write_batch_sender.send(req).await {
            no_fail(e);
        }
        Ok(())
    }

    pub(crate) async fn do_write(this: &Arc<DBInner>, entires: Vec<WriteEntry>) -> Result<()> {
        tracing::debug!("do write, entries");
        this.write_batch_inner(&entires).await?;
        Ok(())
    }

    pub async fn begin_transaction(&self) -> Result<Arc<Transaction>> {
        let write_sender = self.write_batch_sender.clone();

        self.inner.new_txn(write_sender).await
    }

    async fn drop_no_fail(&mut self) {
        let _ = self.send_write_request(WriteRequest::Exit).await;
        let _ = self.flush_closed_sender.send(()).await;

        if let Some(h) = self.write_task.take() {
            let _ = h.await;
        }
        if let Some(h) = self.flush_task.take() {
            let _ = h.await;
        }

        if let Err(e) = unlock_db(&self._options.main_db_dir, &self._options.value_log_dir).await {
            no_fail(e);
        }

        tracing::info!("DB closed");
    }
}

impl Drop for RemDB {
    fn drop(&mut self) {
        futures::executor::block_on(async { self.drop_no_fail().await });
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{options::DBOpenOptions, test_utils::run_async_test};

    #[test]
    fn test_put_and_set() -> anyhow::Result<()> {
        run_async_test(async || -> anyhow::Result<()> {
            let tempdir = tempfile::tempdir()?;

            let db = DBOpenOptions::default()
                .memtable_size_threshold(1)
                .db_path(tempdir.path())
                .value_log_dir(tempdir.path())
                .open()
                .await?;

            db.put(b"key", b"value").await?;
            let value = db.get(b"key").await?;
            assert_eq!(value.unwrap().as_ref(), b"value");

            db.put(b"key", b"key2").await?;
            let value = db.get(b"key").await?;
            assert_eq!(value.unwrap().as_ref(), b"key2");

            Ok(())
        })
    }

    #[test]
    fn test_txn() -> anyhow::Result<()> {
        run_async_test(async || {
            let tempdir = tempfile::tempdir()?;

            let db = DBOpenOptions::default()
                .memtable_size_threshold(1)
                .db_path(tempdir.path())
                .value_log_dir(tempdir.path())
                .open()
                .await?;

            db.put(b"key1", b"1").await?;
            db.put(b"key2", b"2").await?;

            let txn1 = db.begin_transaction().await?;
            db.put(b"key1", b"3").await?;

            let txn2 = db.begin_transaction().await?;
            db.delete(b"key2").await?;
            db.put(b"key3", b"5").await?;

            let txn3 = db.begin_transaction().await?;

            while db.inner.force_flush_immutable_memtable().await.is_ok() {
                dbg!("flush !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            }

            assert_eq!(txn1.read_ts(), 2);
            assert_eq!(txn1.get(b"key1").await?, Some(Bytes::from_static(b"1")));
            assert_eq!(txn1.get(b"key2").await?, Some(Bytes::from_static(b"2")));
            assert_eq!(txn1.get(b"key3").await?, None);

            assert_eq!(txn2.read_ts(), 3);
            assert_eq!(txn2.get(b"key1").await?, Some(Bytes::from_static(b"3")));
            assert_eq!(txn2.get(b"key2").await?, Some(Bytes::from_static(b"2")));
            assert_eq!(txn2.get(b"key3").await?, None);

            assert_eq!(txn3.read_ts(), 5);
            assert_eq!(txn3.get(b"key1").await?, Some(Bytes::from_static(b"3")));
            assert_eq!(txn3.get(b"key2").await?, None);
            assert_eq!(txn3.get(b"key3").await?, Some(Bytes::from_static(b"5")));

            txn1.commit().await?;
            txn2.commit().await?;
            txn3.commit().await?;

            Ok(())
        })
    }

    #[test]
    fn test_immutable_memtable_flush_to_sst() -> anyhow::Result<()> {
        run_async_test(async || {
            let tempdir = tempfile::tempdir()?;

            let db = DBOpenOptions::default()
                .db_path(tempdir.path())
                .value_log_dir(tempdir.path())
                .open()
                .await?;

            db.put(b"key1", b"1").await?;
            db.put(b"key2", b"2").await?;
            db.put(b"key3", b"3").await?;
            db.put(b"key1", b"4").await?;

            db.inner.force_freeze_current_memtable_for_test().await;
            db.inner.force_flush_immutable_memtable().await?;

            assert_eq!(db.get(b"key1").await?, Some(Bytes::from_static(b"4")));
            assert_eq!(db.get(b"key2").await?, Some(Bytes::from_static(b"2")));
            assert_eq!(db.get(b"key3").await?, Some(Bytes::from_static(b"3")));

            Ok(())
        })
    }
}
