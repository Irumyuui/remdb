#![allow(unused)] // TODO: remove this line after finish

use std::path::{Path, PathBuf};

pub mod key;
pub mod value;

pub const VLOF_FILE_SUFFIX: &str = ".vlog";

pub const WAL_FILE_SUFFIX: &str = ".wal";

pub const SST_FILE_SUFFIX: &str = ".sst";

pub fn vlog_format_path(dir: impl AsRef<Path>, fid: u32) -> PathBuf {
    dir.as_ref().join(format!("{:09}{VLOF_FILE_SUFFIX}", fid))
}

pub fn wal_format_path(dir: impl AsRef<Path>, fid: u32) -> PathBuf {
    dir.as_ref().join(format!("{:09}{WAL_FILE_SUFFIX}", fid))
}

pub fn sst_format_path(dir: impl AsRef<Path>, fid: u32) -> PathBuf {
    dir.as_ref().join(format!("{:09}{SST_FILE_SUFFIX}", fid))
}
