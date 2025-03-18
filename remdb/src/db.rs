use std::sync::Arc;

use async_channel::{Receiver, Sender};
use bytes::Bytes;
use tokio::task::JoinHandle;
use tracing::info;

use crate::{
    batch::WriteBatch,
    core::{DBInner, WrireRecord},
    error::{Error, Result, no_fail},
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

enum WriteRequest {
    Batch {
        batch: WriteBatch,
        result_sender: Sender<Result<()>>,
    },
    Exit,
}

impl DBInner {
    async fn start_write_batch_task(
        self: &Arc<Self>,
        write_batch_receiver: Receiver<WriteRequest>,
    ) -> Result<JoinHandle<()>> {
        let this = self.clone();

        let write_task = tokio::spawn(async move {
            info!("Start write task");
            while let Ok(req) = write_batch_receiver.recv().await {
                match req {
                    WriteRequest::Batch {
                        batch,
                        result_sender,
                    } => {
                        let write_result = RemDB::do_write(&this, batch).await;
                        if let Err(e) = result_sender.send(write_result).await {
                            no_fail(e.into());
                        }
                    }
                    WriteRequest::Exit => {
                        info!("Write task closed");
                        break;
                    }
                }
            }
        });

        Ok(write_task)
    }
}

impl RemDB {
    pub async fn open(options: Arc<DBOptions>) -> Result<Self> {
        info!("RemDB begin to open");

        let inner = Arc::new(DBInner::open(options.clone()).await?);
        let (write_batch_sender, write_batch_receiver) = async_channel::unbounded();
        let write_task = inner.start_write_batch_task(write_batch_receiver).await?;
        let (flush_closed_sender, flush_closed_receiver) = async_channel::bounded(1);
        let flush_task = inner.start_flush_task(flush_closed_receiver).await?;

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
        self.inner.get(key).await
    }

    pub async fn scan(&self) {
        // need iter
        todo!()
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let (mut batch, tx, rx) = Self::prepare_write_batch();
        batch.put(key, value);
        self.send_write_request(WriteRequest::Batch {
            batch,
            result_sender: tx,
        })
        .await?;

        Self::handle_write_result(rx).await
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let (mut batch, tx, rx) = Self::prepare_write_batch();
        batch.delete(key);
        self.send_write_request(WriteRequest::Batch {
            batch,
            result_sender: tx,
        })
        .await?;

        Self::handle_write_result(rx).await
    }

    pub async fn write_batch(&self, data: &[WrireRecord<&[u8]>]) -> Result<()> {
        let (mut batch, tx, rx) = Self::prepare_write_batch();
        for b in data.iter() {
            match b {
                WrireRecord::Put(key, value) => batch.put(key, value),
                WrireRecord::Delete(key) => batch.delete(key),
            }
        }
        self.send_write_request(WriteRequest::Batch {
            batch,
            result_sender: tx,
        })
        .await?;

        Self::handle_write_result(rx).await
    }

    async fn send_write_request(&self, req: WriteRequest) -> Result<()> {
        if let Err(e) = self.write_batch_sender.send(req).await {
            no_fail(e.into());
        }
        Ok(())
    }

    async fn do_write(this: &Arc<DBInner>, batch: WriteBatch) -> Result<()> {
        info!("Get write request: {:?}", batch);
        this.write_batch(batch.into_batch().as_ref()).await?;
        Ok(())
    }

    async fn handle_write_result(receiver: Receiver<Result<()>>) -> Result<()> {
        let res = receiver.recv().await;
        if let Ok(res) = res {
            res
        } else {
            Err(Error::Corruption("DB Closed".into()))
        }
    }

    fn prepare_write_batch() -> (WriteBatch, Sender<Result<()>>, Receiver<Result<()>>) {
        let (tx, rx) = async_channel::bounded(1);
        let batch = WriteBatch::default();
        (batch, tx, rx)
    }

    pub async fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn().await
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

            let txn1 = db.new_txn().await?;
            db.put(b"key1", b"3").await?;

            let txn2 = db.new_txn().await?;
            db.delete(b"key2").await?;
            db.put(b"key3", b"5").await?;

            let txn3 = db.new_txn().await?;

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
}
