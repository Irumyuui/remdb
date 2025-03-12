#![allow(unused)]

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use fast_async_mutex::rwlock::RwLock;

use crate::{
    error::{Error, Result},
    key::Seq,
};

pub struct Transaction {
    read_seq: Seq,
    // db
    txn_data: Arc<RwLock<BTreeMap<Bytes, Bytes>>>,
    commited: Arc<AtomicBool>,
}

impl Transaction {
    pub fn check_commit(&self) -> Result<()> {
        if self.commited() {
            Err(Error::Txn("txn has been commited".into()))
        } else {
            Ok(())
        }
    }

    fn commited(&self) -> bool {
        self.commited.load(Ordering::Relaxed)
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        todo!()
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn commit(self: Arc<Self>) -> Result<()> {
        todo!()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        todo!()
    }
}
