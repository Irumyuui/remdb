use std::sync::OnceLock;

use async_channel::Sender;
use tokio::task::JoinHandle;

use crate::{batch::WriteRequest, error::NoFail};

use super::DeleteFileRequest;

/// just init once...
#[derive(Default)]
pub(crate) struct TaskController {
    write_task: OnceLock<JoinHandle<()>>,
    write_batch_sender: OnceLock<Sender<WriteRequest>>,

    flush_task: OnceLock<JoinHandle<()>>,
    flush_closed_sender: OnceLock<Sender<()>>,

    del_task: OnceLock<JoinHandle<()>>,
    del_closed_sender: OnceLock<Sender<DeleteFileRequest>>,
}

impl TaskController {
    pub fn init_write_task(&self, write_task: JoinHandle<()>) -> &Self {
        self.write_task.get_or_init(|| write_task);
        self
    }

    pub fn init_write_batch_sender(&self, write_batch_sender: Sender<WriteRequest>) -> &Self {
        self.write_batch_sender.get_or_init(|| write_batch_sender);
        self
    }

    pub fn init_flush_task(&self, flush_task: JoinHandle<()>) -> &Self {
        self.flush_task.get_or_init(|| flush_task);
        self
    }

    pub fn init_flush_closed_sender(&self, flush_closed_sender: Sender<()>) -> &Self {
        self.flush_closed_sender.get_or_init(|| flush_closed_sender);
        self
    }

    pub fn init_deleted_task(&self, del_task: JoinHandle<()>) -> &Self {
        self.del_task.get_or_init(|| del_task);
        self
    }

    pub fn init_deleted_closed_sender(
        &self,
        del_closed_sender: Sender<DeleteFileRequest>,
    ) -> &Self {
        self.del_closed_sender.get_or_init(|| del_closed_sender);
        self
    }

    pub async fn send_delete_ids(&self, req: DeleteFileRequest) {
        let sender = self.del_closed_sender.get().unwrap();
        sender.send(req).await.to_no_fail();
    }

    pub(crate) fn get_write_batch_sender(&self) -> &Sender<WriteRequest> {
        self.write_batch_sender.get().expect("must inited")
    }

    // pub async fn send_write_batch(
    //     &self,
    //     req: WriteRequest,
    // ) -> std::result::Result<(), async_channel::SendError<WriteRequest>> {
    //     self.get_write_batch_sender().send(req).await
    // }

    pub(crate) async fn send_close_signal(&self) {
        if let Some(s) = self.write_batch_sender.get() {
            s.send(WriteRequest::Exit).await.to_no_fail();
        }
        if let Some(s) = self.flush_closed_sender.get() {
            s.send(()).await.to_no_fail();
        }
        if let Some(s) = self.del_closed_sender.get() {
            s.send(DeleteFileRequest::Exit).await.to_no_fail();
        }
    }

    async fn drop_no_fail(&mut self) {
        // self.send_close_signal().await;

        if let Some(t) = self.write_task.take() {
            t.await.to_no_fail();
        }
        if let Some(t) = self.flush_task.take() {
            t.await.to_no_fail();
        }
        if let Some(t) = self.del_task.take() {
            t.await.to_no_fail();
        }
    }
}

impl Drop for TaskController {
    fn drop(&mut self) {
        futures::executor::block_on(async { self.drop_no_fail().await });
    }
}
