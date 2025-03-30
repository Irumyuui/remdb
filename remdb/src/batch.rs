use std::fmt::Debug;

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

pub enum WrireRecord<T>
where
    T: AsRef<[u8]>,
{
    Put(T, T),
    Delete(T),
}

impl<T> Debug for WrireRecord<T>
where
    T: AsRef<[u8]> + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Put(arg0, arg1) => f.debug_tuple("Put").field(arg0).field(arg1).finish(),
            Self::Delete(arg0) => f.debug_tuple("Delete").field(arg0).finish(),
        }
    }
}
