#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("DB closed: {0}")]
    Closed(Box<dyn std::error::Error>),
}

pub type Result<T> = std::result::Result<T, Error>;
