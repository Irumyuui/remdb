#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Corruption: {0}")]
    Corruption(Box<dyn std::error::Error + Send + Sync>),

    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Decode: {0}")]
    Decode(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
