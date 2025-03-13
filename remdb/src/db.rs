#![allow(unused)]

use std::{future, sync::Arc};

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
    options: Arc<DBOptions>,

    write_task: Option<JoinHandle<()>>,
    write_batch_sender: Sender<WriteRequest>,
}

enum WriteRequest {
    Batch {
        batch: WriteBatch,
        result_sender: Sender<Result<()>>,
    },
    Exit,
}

impl RemDB {
    pub async fn open(options: Arc<DBOptions>) -> Result<Self> {
        info!("RemDB begin to open");

        let inner = Arc::new(DBInner::open(options.clone()).await?);
        let (write_batch_sender, write_batch_receiver) = async_channel::unbounded();
        let mut this = Self {
            inner: inner.clone(),
            write_batch_sender,
            options,
            write_task: None,
        };

        let write_task = tokio::spawn(async move {
            info!("Start write task");
            while let Ok(req) = write_batch_receiver.recv().await {
                match req {
                    WriteRequest::Batch {
                        batch,
                        result_sender,
                    } => {
                        if let Err(write_err) = Self::do_write(&inner, batch).await
                            && let Err(e) = result_sender.send(Err(write_err)).await
                        {
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
        this.write_task.replace(write_task);

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
        batch.delete(key.to_vec());
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
        if let Ok(res) = receiver.recv().await {
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

        todo!()
    }
}

impl Drop for RemDB {
    fn drop(&mut self) {
        futures::executor::block_on(async { self.drop_no_fail().await });
    }
}
