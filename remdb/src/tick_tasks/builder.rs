use std::{collections::VecDeque, sync::Arc, time::Duration};

use async_channel::{Receiver, Sender};
use tokio::time::{Instant, interval_at};

use crate::{
    batch::WriteRequest,
    core::DBInner,
    error::NoFail,
    tick_tasks::{DeletedFiles, FileDeleteAction},
};

use super::DeleteFileRequest;

pub struct Builder {
    db_inner: Arc<DBInner>,
}

pub struct Controller {
    pub(crate) write_req_sender: Option<Sender<WriteRequest>>,
    pub(crate) flush_closed_sender: Option<Sender<()>>,
    pub(crate) deleted_file_sender: Option<Sender<DeleteFileRequest>>,
    pub(crate) compact_closed_sender: Option<Sender<()>>,

    pub(crate) write_task_closed: Option<Receiver<()>>,
    pub(crate) flush_task_closed: Option<Receiver<()>>,
    pub(crate) deleted_file_task_closed: Option<Receiver<()>>,
    pub(crate) compact_task_closed: Option<Receiver<()>>,
}

impl Builder {
    pub fn new(inner: Arc<DBInner>) -> Self {
        Self { db_inner: inner }
    }

    pub fn build(&self) -> Controller {
        let (deleted_file_sender, del_task_closed) = self.start_delete_file_task();
        let (write_req_sender, write_task_closed) = self.start_write_batch_task();
        let (flush_closed_sender, flush_task_closed) = self.start_flush_task();

        let (compact_closed_sender, compact_task_closed) =
            self.start_compact_task(deleted_file_sender.clone());

        Controller {
            write_req_sender: Some(write_req_sender),
            flush_closed_sender: Some(flush_closed_sender),
            deleted_file_sender: Some(deleted_file_sender),
            compact_closed_sender: Some(compact_closed_sender),

            write_task_closed: Some(write_task_closed),
            flush_task_closed: Some(flush_task_closed),
            deleted_file_task_closed: Some(del_task_closed),
            compact_task_closed: Some(compact_task_closed),
        }
    }

    fn start_delete_file_task(&self) -> (Sender<DeleteFileRequest>, Receiver<()>) {
        let inner = self.db_inner.clone();
        let (req_sender, req_receiver) = async_channel::unbounded();
        let (closed_sender, closed_reciver) = async_channel::bounded::<()>(1);

        tokio::spawn(async move {
            tracing::info!("Start delete file task");

            tracing::info!("Start delete file task");
            let duration = Duration::from_secs(60);
            let mut interval_task = interval_at(Instant::now() + duration, duration);

            let mut deleted_queue = VecDeque::new();

            loop {
                tokio::select! {
                    ins = interval_task.tick() => {
                        tracing::info!("Delete file task tick, {:?}", ins);
                        inner.handle_delete_file_action(&mut deleted_queue).await.to_no_fail();
                    },
                    task = req_receiver.recv() => {
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

            closed_sender.send(()).await.to_no_fail();
        });

        (req_sender, closed_reciver)
    }

    fn start_write_batch_task(&self) -> (Sender<WriteRequest>, Receiver<()>) {
        let inner = self.db_inner.clone();
        let (sender, receiver) = async_channel::unbounded::<WriteRequest>();
        let (closed_sender, closed_reciver) = async_channel::bounded::<()>(1);

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

            closed_sender.send(()).await.to_no_fail();
        });

        (sender, closed_reciver)
    }

    fn start_flush_task(&self) -> (Sender<()>, Receiver<()>) {
        let inner = self.db_inner.clone();
        let (req_sender, req_receiver) = async_channel::bounded::<()>(1);
        let (closed_sender, closed_reciver) = async_channel::bounded::<()>(1);

        tokio::spawn(async move {
            tracing::info!("Start flush task");

            let duration = Duration::from_secs(inner.options.compact_tick_sec);
            let mut interval_task = interval_at(Instant::now() + duration, duration);

            loop {
                tokio::select! {
                    ins = interval_task.tick() => {
                        tracing::info!("Flush task tick, {:?}", ins);
                        inner.try_flush_immutable_memtable().await.to_no_fail();
                    }
                    _ = req_receiver.recv() => {
                        break;
                    }
                }
            }

            tracing::info!("Flush task closed");
            closed_sender.send(()).await.to_no_fail();
        });

        (req_sender, closed_reciver)
    }

    fn start_compact_task(
        &self,
        delete_file_sender: Sender<DeleteFileRequest>,
    ) -> (Sender<()>, Receiver<()>) {
        let inner = self.db_inner.clone();
        let (req_sender, req_receiver) = async_channel::bounded::<()>(1);
        let (closed_sender, closed_reciver) = async_channel::bounded::<()>(1);

        tokio::spawn(async move {
            let duration = Duration::from_secs(inner.options.compact_tick_sec);
            let mut interval_task = interval_at(Instant::now() + duration, duration);

            let do_compact = async || match inner.try_compact_sstables().await {
                Ok(ids) if !ids.is_empty() => {
                    tracing::info!("Compact task, compacted sstables: {:?}", ids);
                    let req = DeleteFileRequest::Action(FileDeleteAction {
                        commit_txn: inner.mvcc.last_commit_ts().await,
                        files: DeletedFiles::Tables(ids),
                    });
                    delete_file_sender.send(req).await.to_no_fail();
                }
                Ok(_) => {}
                res => res.to_no_fail(),
            };

            loop {
                tokio::select! {
                    ins = interval_task.tick() => {
                        do_compact().await;
                    }
                    _ = req_receiver.recv() => {
                        break;
                    }
                }
            }

            closed_sender.send(()).await.to_no_fail();
        });

        (req_sender, closed_reciver)
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
        if let Some(sender) = &self.compact_closed_sender {
            sender.send(()).await.to_no_fail();
        }
        self.wait_task_closed().await;
    }

    async fn wait_task_closed(&self) {
        if let Some(receiver) = &self.write_task_closed {
            receiver.recv().await.to_no_fail();
        }
        if let Some(receiver) = &self.flush_task_closed {
            receiver.recv().await.to_no_fail();
        }
        if let Some(receiver) = &self.deleted_file_task_closed {
            receiver.recv().await.to_no_fail();
        }
        if let Some(receiver) = &self.compact_task_closed {
            receiver.recv().await.to_no_fail();
        }
    }
}
