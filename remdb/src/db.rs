#![allow(unused)]

use std::sync::Arc;

use bytes::Bytes;
use tokio::task::JoinHandle;
use tracing::info;

use crate::{
    batch::WriteBatch,
    core::{DBInner, WriteOption},
    error::{Result, no_fail},
    options::DBOptions,
};

pub struct RemDB {
    inner: Arc<DBInner>,
    batch_sender: async_channel::Sender<WriteRequest>,
    options: Arc<DBOptions>,
    write_task: Option<JoinHandle<()>>,
}

enum WriteRequest {
    Batch(WriteBatch),
    Exit,
}

impl RemDB {
    pub async fn open(options: Arc<DBOptions>) -> Result<Self> {
        info!("RemDB begin to open");

        let inner = Arc::new(DBInner::open(options.clone()).await?);
        let (tx, rx) = async_channel::unbounded();
        let mut this = Self {
            inner: inner.clone(),
            batch_sender: tx,
            options,
            write_task: None,
        };

        let write_task = tokio::spawn(async move {
            info!("Start write task");
            while let Ok(req) = rx.recv().await {
                match req {
                    WriteRequest::Batch(batch) => {
                        if let Err(e) = Self::do_write(&inner, batch).await {
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
        todo!()
    }

    pub async fn scan(&self) {
        // need iter
        todo!()
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.put(key, value);
        self.send_write_request(WriteRequest::Batch(batch)).await
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.delete(key.to_vec());
        self.send_write_request(WriteRequest::Batch(batch)).await
    }

    pub async fn write_batch(&self, data: &[WriteOption<&[u8]>]) -> Result<()> {
        let mut batch = WriteBatch::default();
        for b in data.iter() {
            match b {
                WriteOption::Put(key, value) => batch.put(key, value),
                WriteOption::Delete(key) => batch.delete(key),
            }
        }
        self.send_write_request(WriteRequest::Batch(batch)).await
    }

    async fn send_write_request(&self, req: WriteRequest) -> Result<()> {
        if let Err(e) = self.batch_sender.send(req).await {
            no_fail(e.into());
        }
        Ok(())
    }

    async fn do_write(this: &Arc<DBInner>, batch: WriteBatch) -> Result<()> {
        info!("Get write request: {:?}", batch);

        this.write_batch(batch.into_batch().as_ref());

        todo!()
    }
}
