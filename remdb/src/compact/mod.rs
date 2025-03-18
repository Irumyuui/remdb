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
                        tracing::info!("flush task tick, instants: {:?}", ins);
                        if let Err(e) = this.try_flush_immutable_memtable().await {
                            no_fail(e.into());
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
}
