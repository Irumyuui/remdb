use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_channel::Receiver;
use tokio::task::JoinHandle;

use crate::{
    core::DBInner,
    error::{Result, no_fail},
};

pub mod level;

impl DBInner {
    pub async fn start_flush_task(
        self: &Arc<Self>,
        closed: Receiver<()>,
    ) -> Result<JoinHandle<()>> {
        let this = self.clone();
        let handle = tokio::spawn(async move {
            tracing::info!("Start flush task");

            let duration = Duration::from_secs(this.options.compact_tick_sec);
            let mut interval_task =
                tokio::time::interval_at((Instant::now() + duration).into(), duration);

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
            let mut interval_task =
                tokio::time::interval_at((Instant::now() + duration).into(), duration);

            loop {
                tokio::select! {
                    ins = interval_task.tick() => {
                        tracing::info!("flush task tick, {:?}", ins);
                        if let Err(e) = this.try_compact_tables().await {
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

    /// If compact is needed, do compact, and return true.
    async fn try_compact_tables(&self) -> Result<bool> {
        // 因为在单独任务下进行压实，所以很多可以直接不考虑并行问题
        // 但是需要考虑的一点是：压实后可能存在新的 level 0 和 memtable，所以需要获取一下新的 state

        // 先计算有哪些 table 需要压实
        // 然后创建新文件

        todo!()
    }

    #[allow(unused)]
    async fn do_compact(&self /* some */) -> Result<()> {
        todo!()
    }
}
