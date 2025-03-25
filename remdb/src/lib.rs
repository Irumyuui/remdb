#![feature(let_chains)]

mod batch;
mod compact;
mod core;
mod db;
mod format;
mod fs;
mod manifest;
mod memtable;
mod mvcc;
mod table;
mod tick_tasks;
mod utils;
mod value_log;

#[cfg(test)]
pub(crate) mod test_utils;

pub mod error;
pub mod iterator;
pub mod options;

pub use db::RemDB;

pub mod prelude {
    pub use crate::{db::RemDB, error::*, iterator::*, options::*};
}

// TODO: set a feature flag to enable/disable mimalloc
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
