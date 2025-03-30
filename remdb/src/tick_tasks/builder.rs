use std::{collections::VecDeque, sync::Arc, time::Duration};

use async_channel::Sender;
use tokio::time::Instant;

use crate::{batch::WriteRequest, core::DBInner, error::NoFail};

use super::DeleteFileRequest;

pub struct Builder {
    db_inner: Arc<DBInner>,
}

pub struct Controller {
    pub(crate) write_req_sender: Option<Sender<WriteRequest>>,
    pub(crate) flush_closed_sender: Option<Sender<()>>,
    pub(crate) deleted_file_sender: Option<Sender<DeleteFileRequest>>,
}

impl Builder {
    pub fn new(inner: Arc<DBInner>) -> Self {
        Self { db_inner: inner }
    }

    pub fn build(&self) -> Controller {
        let deleted_file_sender = self.start_delete_file_task();
        let write_req_sender = self.start_write_batch_task();
        let flush_closed_sender = self.start_flush_task();

        Controller {
            write_req_sender: Some(write_req_sender),
            flush_closed_sender: Some(flush_closed_sender),
            deleted_file_sender: Some(deleted_file_sender),
        }
    }

    fn start_delete_file_task(&self) -> Sender<DeleteFileRequest> {
        let inner = self.db_inner.clone();
        let (s, t) = async_channel::unbounded();

        tokio::spawn(async move {
            tracing::info!("Start delete file task");

            tracing::info!("Start delete file task");
            let duration = Duration::from_secs(60);
            let mut interval_task = tokio::time::interval_at(Instant::now() + duration, duration);

            let mut deleted_queue = VecDeque::new();

            loop {
                tokio::select! {
                    ins = interval_task.tick() => {
                        tracing::info!("Delete file task tick, {:?}", ins);
                        inner.handle_delete_file_action(&mut deleted_queue).await.to_no_fail();
                    },
                    task = t.recv() => {
                        match task {
                            Ok(DeleteFileRequest::Action(action)) => {
                                deleted_queue.push_back(action);
                            }
                            Ok(DeleteFileRequest::Exit) => {
                                tracing::info!("Delete file task closed");
                                break;
                            }
                            Err(e) => {
                                panic!("Receive delete file request error: {:?}", e);
                            }
                        }
                    }
                }
            }

            // if exit, try to remove all files
            inner
                .handle_delete_file_action(&mut deleted_queue)
                .await
                .to_no_fail();
        });

        s
    }

    fn start_write_batch_task(&self) -> Sender<WriteRequest> {
        let inner = self.db_inner.clone();
        let (sender, receiver) = async_channel::unbounded::<WriteRequest>();

        tokio::spawn(async move {
            tracing::info!("Start write batch task");

            while let Ok(req) = receiver.recv().await {
                match req {
                    WriteRequest::Batch {
                        entries,
                        result_sender,
                    } => {
                        let result = inner.write_batch_inner(&entries).await;
                        result_sender.send(result).await.to_no_fail();
                    }
                    WriteRequest::Exit => {
                        tracing::info!("Write task closed");
                        break;
                    }
                }
            }
        });

        sender
    }

    fn start_flush_task(&self) -> Sender<()> {
        let inner = self.db_inner.clone();
        let (s, t) = async_channel::bounded::<()>(1);

        tokio::spawn(async move {
            tracing::info!("Start flush task");

            let duration = Duration::from_secs(inner.options.compact_tick_sec);
            let mut interval_task = tokio::time::interval_at(Instant::now() + duration, duration);

            loop {
                tokio::select! {
                    ins = interval_task.tick() => {
                        tracing::info!("Flush task tick, {:?}", ins);
                        inner.try_flush_immutable_memtable().await.to_no_fail();
                    }
                    _ = t.recv() => {
                        break;
                    }
                }
            }

            tracing::info!("Flush task closed");
        });

        s
    }
}

impl Controller {
    pub async fn send_closed_signal(&self) {
        if let Some(sender) = &self.write_req_sender {
            sender.send(WriteRequest::Exit).await.to_no_fail();
        }
        if let Some(sender) = &self.flush_closed_sender {
            sender.send(()).await.to_no_fail();
        }
        if let Some(sender) = &self.deleted_file_sender {
            sender.send(DeleteFileRequest::Exit).await.to_no_fail();
        }
    }
}
