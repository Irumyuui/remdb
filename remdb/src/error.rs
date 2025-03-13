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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) fn no_fail(err: Box<dyn std::error::Error + Send + Sync>) {
    tracing::error!("{}", err);
}
