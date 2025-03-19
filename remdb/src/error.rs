#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Corruption: {0}")]
    Corruption(Box<dyn std::error::Error + Send + Sync>),

    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Decode: {0}")]
    Decode(String),

    #[error("Txn: {0}")]
    Txn(String),

    #[error("flush error: {0}")]
    MemTableFlush(Box<dyn std::error::Error + Send + Sync>),

    #[error("Table recover: {0}")]
    TableRecover(Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) fn no_fail(err: Box<dyn std::error::Error + Send + Sync>) {
    tracing::error!("not failid, but found error: {}", err);
}

impl Error {
    pub(crate) fn table_recover<T, E: Into<Box<dyn std::error::Error + Send + Sync>>>(
        err: E,
    ) -> Result<T, Self> {
        Err(Self::TableRecover(err.into()))
    }
}
