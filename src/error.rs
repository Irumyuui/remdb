#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Log decode error: {0}")]
    VLogDecode(String),
}

pub type Result<T> = std::result::Result<T, Error>;
