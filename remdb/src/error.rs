use std::path::Path;

use crate::format::key::Seq;

#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("Corruption: {0}")]
    Corruption(Box<dyn std::error::Error + Send + Sync>),

    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Decode: {0}")]
    Decode(String),

    #[error("Txn: {0}")]
    Txn(String),

    #[error("Transaction already commited, read ts: {0}")]
    TxnCommited(Seq),

    #[error("flush error: {0}")]
    MemTableFlush(Box<dyn std::error::Error + Send + Sync>),

    #[error("Table recover: {0}")]
    TableRecover(Box<dyn std::error::Error + Send + Sync>),

    #[error("Already used: {0}")]
    Locked(String),

    #[error("Immutable memtable list is empty")]
    ImmutableMemtableNotFound,

    #[error("Checksum mismatch")]
    ChecksumMismatch,

    #[error(
        "Serializable failed, current ts {current_ts} read a modified key by forward ts {forward_ts}"
    )]
    TxnSerializableFailed { current_ts: Seq, forward_ts: Seq },
}

pub type KvResult<T, E = KvError> = std::result::Result<T, E>;

pub(crate) fn no_fail<E>(err: E)
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    tracing::error!("not failid, but found error: {}", err.into());
}

pub(crate) trait NoFail {
    fn to_no_fail(self);
}

impl<T, E> NoFail for std::result::Result<T, E>
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fn to_no_fail(self) {
        if let Err(e) = self {
            no_fail(e.into());
        }
    }
}

impl KvError {
    pub(crate) fn table_recover<T, E: Into<Box<dyn std::error::Error + Send + Sync>>>(
        err: E,
    ) -> KvResult<T, Self> {
        Err(Self::TableRecover(err.into()))
    }

    pub(crate) fn locked<T>(path: impl AsRef<Path>) -> KvResult<T> {
        Err(Self::Locked(format!("get lock file: {:?}", path.as_ref())))
    }
}
