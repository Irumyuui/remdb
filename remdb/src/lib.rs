#![feature(let_chains)]

mod batch;
mod core;
mod db;
mod format;
mod fs;
mod key;
mod memtable;
mod mvcc;
mod value;
mod value_log;

#[cfg(test)]
pub(crate) mod test_utils;

pub mod error;
pub mod iterator;
pub mod options;

pub use db::RemDB;
