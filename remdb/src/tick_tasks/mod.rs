#![allow(unused)] // TODO: remove it.......

use std::{collections::VecDeque, sync::Arc, time::Duration};

use async_channel::Receiver;
use tokio::{task::JoinHandle, time::Instant};

use crate::{
    RemDB,
    batch::WriteRequest,
    core::DBInner,
    error::{KvResult, NoFail},
    format::{key::Seq, sst_format_path, vlog_format_path},
};

pub mod builder;

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

impl DeleteFileRequest {
    pub fn new_delete_vlog(commit_txn: Seq, ids: Vec<u32>) -> Self {
        Self::Action(FileDeleteAction {
            commit_txn,
            files: DeletedFiles::ValueLogs(ids),
        })
    }

    pub fn new_delete_table(commit_txn: Seq, ids: Vec<u32>) -> Self {
        Self::Action(FileDeleteAction {
            commit_txn,
            files: DeletedFiles::Tables(ids),
        })
    }
}

impl DBInner {
    async fn handle_delete_file_action(
        &self,
        actions: &mut VecDeque<FileDeleteAction>,
    ) -> KvResult<()> {
        let watermark = self.mvcc.watermark().await;
        while let Some(tasks) = actions.front() {
            if tasks.commit_txn <= watermark {
                let action = actions.pop_front().unwrap();
                tracing::info!("Delete file action: {:?}", action);
                match action.files {
                    DeletedFiles::ValueLogs(items) => {
                        for id in items {
                            self.vlogs.remove_deleted_vlog_file(id).await;
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
