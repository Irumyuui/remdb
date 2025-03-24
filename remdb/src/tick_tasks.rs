use std::{sync::Arc, time::Duration};

use async_channel::Receiver;
use tokio::{task::JoinHandle, time::Instant};

use crate::{
    RemDB,
    core::DBInner,
    db::WriteRequest,
    error::{Result, no_fail},
};

impl DBInner {
    pub async fn start_flush_task(
        self: &Arc<Self>,
        closed: Receiver<()>,
    ) -> Result<JoinHandle<()>> {
        let this = self.clone();
        let handle = tokio::spawn(async move {
            tracing::info!("Start flush task");

            let duration = Duration::from_secs(this.options.compact_tick_sec);
            let mut interval_task = tokio::time::interval_at(Instant::now() + duration, duration);

            loop {
                tokio::select! {
                    ins = interval_task.tick() => {
                        tracing::info!("Flush task tick, {:?}", ins);
                        if let Err(e) = this.try_flush_immutable_memtable().await {
                            no_fail(e);
                        }
                    }
                    _ = closed.recv() => {
                        break;
                    }
                }
            }

            tracing::info!("Flush task closed");
        });
        Ok(handle)
    }

    pub async fn start_compact_task(
        self: &Arc<Self>,
        closed: Receiver<()>,
    ) -> Result<JoinHandle<()>> {
        let this = self.clone();

        let handle = tokio::spawn(async move {
            let duration = Duration::from_secs(this.options.compact_tick_sec);
            let mut interval_task = tokio::time::interval_at(Instant::now() + duration, duration);

            loop {
                tokio::select! {
                    ins = interval_task.tick() => {
                        tracing::info!("flush task tick, {:?}", ins);
                        if let Err(e) = this.try_compact_sstables().await {
                            no_fail(e);
                        }
                    }
                    _ = closed.recv() => {
                        break;
                    }
                }
            }

            tracing::info!("Compact task closed");
        });

        Ok(handle)
    }

    pub(crate) async fn start_write_batch_task(
        self: &Arc<Self>,
        write_batch_receiver: Receiver<WriteRequest>,
    ) -> Result<JoinHandle<()>> {
        let this = self.clone();

        let write_task = tokio::spawn(async move {
            tracing::info!("Start write task");
            while let Ok(req) = write_batch_receiver.recv().await {
                match req {
                    WriteRequest::Batch {
                        batch,
                        result_sender,
                    } => {
                        let write_result = RemDB::do_write(&this, batch).await;
                        if let Err(e) = result_sender.send(write_result).await {
                            no_fail(e);
                        }
                    }
                    WriteRequest::Exit => {
                        tracing::info!("Write task closed");
                        break;
                    }
                }
            }
        });

        Ok(write_task)
    }
}
