use std::sync::Arc;

use bytes::Bytes;
use tracing::info;

use crate::{
    core::{DBInner, WrireRecord},
    error::{NoFail, Result},
    format::{lock_db, unlock_db},
    mvcc::transaction::Transaction,
    options::DBOptions,
};

pub struct RemDB {
    inner: Arc<DBInner>,
    _options: Arc<DBOptions>,

    // task_controller: Arc<TaskController>,
    controller: Arc<crate::tick_tasks::builder::Controller>,
}

impl RemDB {
    pub async fn open(options: Arc<DBOptions>) -> Result<Self> {
        info!("RemDB begin to open");

        lock_db(&options.main_db_dir, &options.value_log_dir).await?;

        let inner = Arc::new(DBInner::open(options.clone()).await?);

        let controller = crate::tick_tasks::builder::Builder::new(inner.clone()).build();
        let this = Self {
            inner: inner.clone(),
            _options: options,
            controller: Arc::new(controller),
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

    pub async fn begin_transaction(&self) -> Result<Arc<Transaction>> {
        self.inner
            .new_txn(self.controller.write_req_sender.as_ref().unwrap().clone())
            .await
    }

    async fn drop_no_fail(&mut self) {
        self.controller.send_closed_signal().await;
        unlock_db(&self._options.main_db_dir, &self._options.value_log_dir)
            .await
            .to_no_fail();

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
