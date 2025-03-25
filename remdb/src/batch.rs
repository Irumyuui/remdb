use async_channel::Sender;
use bytes::Bytes;

use crate::{error::Result, format::key::KeyBytes};

#[allow(dead_code)]
#[derive(Debug)]
pub enum WriteRequest {
    Batch {
        entries: Vec<WriteEntry>,
        result_sender: Sender<Result<()>>,
    },
    Exit,
}

#[derive(Debug)]
pub struct WriteEntry {
    pub(crate) key: KeyBytes,
    pub(crate) value: Bytes,
}

// #[derive(Debug)]
// pub struct WriteBatch {
//     pub(crate) entries: Vec<WriteEntry>,
// }

// impl WriteBatch {
//     pub fn with_capacity(n: usize) -> Self {
//         Self {
//             entries: Vec::with_capacity(n),
//         }
//     }

//     pub fn add(&mut self, key: KeyBytes, value: Bytes) {
//         self.entries.push(WriteEntry { key, value });
//     }
// }
