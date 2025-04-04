use std::fmt::Debug;

use async_channel::{Receiver, Sender};
use bytes::Bytes;

use crate::{error::KvResult, format::key::KeyBytes};

#[derive(Debug)]
pub enum WriteRequest {
    Batch {
        entries: Vec<WriteEntry>,
        result_sender: Sender<KvResult<()>>,
    },
    Exit,
}

impl WriteRequest {
    pub fn new_batch(entires: Vec<WriteEntry>) -> (Self, Receiver<KvResult<()>>) {
        let (result_sender, result_receiver) = async_channel::bounded::<KvResult<()>>(1);
        (
            Self::Batch {
                entries: entires,
                result_sender,
            },
            result_receiver,
        )
    }
}

#[derive(Debug)]
pub struct WriteEntry {
    pub(crate) key: KeyBytes,
    pub(crate) value: Bytes,
}

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
