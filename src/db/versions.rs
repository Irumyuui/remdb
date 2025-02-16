use std::{
    collections::VecDeque,
    sync::{atomic::AtomicU64, Arc},
};

use parking_lot::RwLock;

use crate::memtable::format::ImplMemTable;

use super::{core::CoreData, format::FileId};

#[derive(Clone)]
pub struct Version<M: ImplMemTable> {
    inner: Arc<VersionInternal<M>>,
}

impl<M: ImplMemTable> Version<M> {
    pub(crate) fn new(
        core_data: CoreData<M>,
        new_files: Vec<FileId>,
        deleted_files: Vec<FileId>,
    ) -> Self {
        Self {
            inner: Arc::new(VersionInternal {
                core_data,
                new_files,
                deleted_files,
            }),
        }
    }
}

impl<M: ImplMemTable> Version<M> {
    fn ref_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    pub(crate) fn core_data(&self) -> &CoreData<M> {
        &self.inner.core_data
    }
}

struct VersionInternal<M: ImplMemTable> {
    core_data: CoreData<M>,

    new_files: Vec<FileId>,
    deleted_files: Vec<FileId>,
}

struct VersionSetInternal<M: ImplMemTable> {
    versions: RwLock<VecDeque<Version<M>>>,
}

impl<M: ImplMemTable> VersionSetInternal<M> {
    pub fn new() -> Self {
        Self {
            versions: RwLock::new(VecDeque::new()),
        }
    }

    pub fn current(&self) -> Option<Version<M>> {
        self.versions.read().back().cloned()
    }

    pub fn push(&self, version: Version<M>) {
        self.versions.write().push_back(version);
    }

    pub fn pop_deleted_versions(&self) -> Vec<Version<M>> {
        let mut guard = self.versions.write();
        let mut deleted_versions = Vec::new();
        while !guard.is_empty() {
            if guard.front().unwrap().ref_count() == 1 {
                deleted_versions.push(guard.pop_front().unwrap());
            } else {
                break;
            }
        }
        deleted_versions
    }
}

#[derive(Clone)]
pub struct VersionSet<M: ImplMemTable> {
    inner: Arc<VersionSetInternal<M>>,
}

impl<M: ImplMemTable> VersionSet<M> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(VersionSetInternal::new()),
        }
    }

    pub fn current(&self) -> Option<Version<M>> {
        self.inner.current()
    }

    pub fn push(&self, version: Version<M>) {
        self.inner.push(version);
    }

    pub fn pop_deleted_versions(&self) -> Vec<Version<M>> {
        self.inner.pop_deleted_versions()
    }
}
