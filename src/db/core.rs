use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    },
    vec,
};

use bytes::Bytes;
use crossbeam::{
    channel::{Receiver, Sender},
    select,
};
use parking_lot::Mutex;

use crate::{
    db::{
        format::{FileId, FileType},
        versions::Version,
    },
    error::{Error, Result},
    memtable::format::ImplMemTable,
    options::Options,
};

use super::versions::VersionSet;

pub struct CoreData<M: ImplMemTable> {
    mem: M,
    imms: VecDeque<M>,
}

pub enum WriteTask {
    Put(Bytes, Bytes, Sender<()>),
    Delete(Bytes, Sender<()>),
}

pub struct Core<M: ImplMemTable> {
    flush_lock: Mutex<()>,
    states: VersionSet<M>,

    options: Arc<Options>,

    write_task_sender: Sender<WriteTask>,
    close_sender: Sender<()>,

    next_sst_id: AtomicUsize,

    is_closed: AtomicBool,
}

impl<M: ImplMemTable> Core<M> {
    pub fn new(
        options: Arc<Options>,
        write_task_sender: Sender<WriteTask>,
        close_sender: Sender<()>,
    ) -> Result<Self> {
        // TODO: Recover
        let states = VersionSet::new();
        states.push(Version::new(
            CoreData {
                mem: M::create(
                    FileId {
                        ty: FileType::MemTable,
                        id: 0,
                        path: "".into(),
                    },
                    options.clone(),
                ),
                imms: VecDeque::new(),
            },
            vec![],
            vec![],
        ));

        let next_sst_id = AtomicUsize::new(1);

        Ok(Self {
            flush_lock: Mutex::new(()),
            states,
            options,
            write_task_sender,
            close_sender,
            is_closed: AtomicBool::new(false),
            next_sst_id,
        })
    }

    fn make_room_for_write(&self, force: bool) -> Result<()> {
        todo!()
    }

    fn check_closed(&self) -> Result<()> {
        let closed = self.is_closed.load(std::sync::atomic::Ordering::Relaxed);
        if closed {
            tracing::error!("DB is closed");
            return Err(Error::Closed("DB is closed".into()));
        }
        Ok(())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_closed()?;

        let (sender, receiver) = crossbeam::channel::bounded(1);
        let task = WriteTask::Put(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
            sender,
        );
        self.write_task_sender
            .send(task)
            .map_err(|e| Error::Closed(e.into()))?;
        receiver.recv().map_err(|e| Error::Closed(e.into()))?;
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.check_closed()?;

        let (sender, receiver) = crossbeam::channel::bounded(1);
        let task = WriteTask::Delete(Bytes::copy_from_slice(key), sender);
        self.write_task_sender
            .send(task)
            .map_err(|e| Error::Closed(e.into()))?;
        receiver.recv().map_err(|e| Error::Closed(e.into()))?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.check_closed()?;
        let result = self.states.current().unwrap().core_data().mem.get(key);
        Ok(result)
    }

    pub fn close(&self) -> Result<()> {
        self.check_closed()?;
        self.close_sender
            .send(())
            .map_err(|e| Error::Closed(e.into()))?;

        self.is_closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::info!("Core closed");

        Ok(())
    }
}

impl<M: ImplMemTable> Core<M> {
    pub fn do_write(&self, task_recv: Receiver<WriteTask>, close_recv: Receiver<()>) {
        tracing::info!("core write thread started");

        loop {
            select! {
                recv(task_recv) -> task => {
                    self.write_task(task.unwrap());
                }
                recv(close_recv) -> sign => {
                    if sign.is_err() {
                        tracing::warn!("WTF: {sign:?}");
                    }
                    tracing::info!("core write thread stopped");
                    return;
                }
            }
        }
    }

    pub fn write_task(&self, task: WriteTask) {
        let current = self.states.current().unwrap();

        match task {
            WriteTask::Put(key, value, sender) => {
                current.core_data().mem.put(key, value);
                sender.send(()).unwrap();
            }
            WriteTask::Delete(key, sender) => {
                current.core_data().mem.put(key, Bytes::new());
                sender.send(()).unwrap();
            }
        }
    }
}
