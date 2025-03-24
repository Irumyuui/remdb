use std::{sync::Arc, time::Duration};

use async_channel::Receiver;
use tokio::{task::JoinHandle, time::Instant};

use crate::{
    RemDB,
    core::DBInner,
    db::WriteRequest,
    error::{NoFail, Result},
};

impl DBInner {
    /// 启动一个 memtable flush 任务
    pub(crate) async fn start_flush_task(
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
                        this.try_flush_immutable_memtable().await.to_no_fail();
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

    /// 启动一个 compact 任务
    #[allow(unused)]
    pub(crate) async fn start_compact_task(
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
                        this.try_compact_sstables().await.to_no_fail();
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

    /// 单独的写任务，因为需要保证写任务一定是在一个线程中
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
                        result_sender.send(write_result).await.to_no_fail();
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
