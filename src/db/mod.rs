use core::{Core, WriteTask};
use std::{
    sync::{Arc, atomic::AtomicBool},
    thread::JoinHandle,
};

use bytes::Bytes;

use crate::{error::Result, memtable::skip_list_impl::MemTable, options::Options};

pub mod core;
pub mod format;
pub mod versions;

pub struct RemDB {
    inner: Arc<Core<MemTable>>,

    write_thread: Arc<Option<JoinHandle<()>>>,
    closed: Arc<AtomicBool>,
}

impl RemDB {
    pub fn open(options: Arc<Options>) -> Result<Self> {
        let (task_sender, task_recv) = crossbeam::channel::unbounded();
        let (close_sender, close_recv) = crossbeam::channel::bounded(1);

        let core = Arc::new(Core::new(options, task_sender, close_sender)?);
        let write_thread = Self::run_write_thread(core.clone(), task_recv, close_recv);

        tracing::info!("DB opened");
        Ok(Self {
            inner: core,
            write_thread: Arc::new(Some(write_thread)),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    fn run_write_thread(
        core: Arc<Core<MemTable>>,
        task_recv: crossbeam::channel::Receiver<WriteTask>,
        close_recv: crossbeam::channel::Receiver<()>,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name("write_thread".into())
            .spawn(move || {
                core.do_write(task_recv, close_recv);
            })
            .unwrap()
    }

    pub fn close(&mut self) -> Result<()> {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::warn!("DB is already closed");
            return Ok(());
        }

        self.inner.close()?;

        let thread = std::mem::replace(&mut self.write_thread, Arc::new(None));
        Arc::into_inner(thread).flatten().unwrap().join().unwrap();

        self.closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::info!("DB closed");

        Ok(())
    }

    fn drop_no_fail(&mut self) {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        if let Err(e) = self.close() {
            tracing::error!("Failed to close DB: {:?}", e);
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }
}

impl Drop for RemDB {
    fn drop(&mut self) {
        self.drop_no_fail();
    }
}
