#![allow(unused)] // TODO: remove it.......

use std::{collections::VecDeque, sync::Arc, time::Duration};

use async_channel::Receiver;
use tokio::{task::JoinHandle, time::Instant};

use crate::{
    RemDB,
    batch::WriteRequest,
    core::DBInner,
    error::{NoFail, Result},
    format::{key::Seq, sst_format_path, vlog_format_path},
};

pub mod task_controller;

impl DBInner {
    /// 启动一个 memtable flush 任务
    pub(crate) async fn register_flush_task(
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
    pub(crate) async fn register_compact_task(
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
    pub(crate) async fn register_write_batch_task(
        self: &Arc<Self>,
        write_batch_receiver: Receiver<WriteRequest>,
    ) -> Result<JoinHandle<()>> {
        let this = self.clone();

        let write_task = tokio::spawn(async move {
            tracing::info!("Start write task");
            while let Ok(req) = write_batch_receiver.recv().await {
                match req {
                    WriteRequest::Batch {
                        entries,
                        result_sender,
                    } => {
                        let write_result = RemDB::do_write(&this, entries).await;
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

#[derive(Debug)]
pub enum DeletedFiles {
    ValueLogs(Vec<u32>),
    Tables(Vec<u32>),
}

#[derive(Debug)]
pub struct FileDeleteAction {
    commit_txn: Seq,
    files: DeletedFiles,
}

#[derive(Debug)]
pub enum DeleteFileRequest {
    Action(FileDeleteAction),
    Exit,
}

impl DBInner {
    #[allow(unused)]
    pub(crate) async fn register_delete_file_task(
        self: &Arc<Self>,
        deleted_req_receiver: Receiver<DeleteFileRequest>,
    ) -> Result<JoinHandle<()>> {
        let this = self.clone();

        let handle = tokio::spawn(async move {
            tracing::info!("Start delete file task");
            let duration = Duration::from_secs(60);
            let mut interval_task = tokio::time::interval_at(Instant::now() + duration, duration);

            let mut deleted_queue = VecDeque::new();

            loop {
                tokio::select! {
                    ins = interval_task.tick() => {
                        tracing::info!("Delete file task tick, {:?}", ins);
                        this.handle_delete_file_action(&mut deleted_queue).await.to_no_fail();
                    },
                    task = deleted_req_receiver.recv() => {
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
            this.handle_delete_file_action(&mut deleted_queue)
                .await
                .to_no_fail();
        });

        Ok(handle)
    }

    async fn handle_delete_file_action(
        &self,
        actions: &mut VecDeque<FileDeleteAction>,
    ) -> Result<()> {
        let watermark = self.mvcc.watermark().await;
        while let Some(tasks) = actions.front() {
            if tasks.commit_txn <= watermark {
                let action = actions.pop_front().unwrap();

                tracing::info!("Delete file action: {:?}", action);

                // required already removed on db
                match action.files {
                    DeletedFiles::ValueLogs(items) => {
                        for id in items {
                            let path = vlog_format_path(&self.options.value_log_dir, id);
                            std::fs::remove_file(&path).to_no_fail(); // is it need async?
                        }
                    }
                    DeletedFiles::Tables(items) => {
                        for id in items {
                            let path = sst_format_path(&self.options.main_db_dir, id);
                            std::fs::remove_file(&path).to_no_fail();
                        }
                    }
                }
            } else {
                break;
            }
        }

        tracing::info!("Delete finish");

        Ok(())
    }
}
