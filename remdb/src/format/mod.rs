#![allow(unused)] // TODO: remove this line after finish

use std::path::{Path, PathBuf};

use crate::error::{Error, Result, no_fail};

pub mod key;
pub mod value;

pub const VLOF_FILE_SUFFIX: &str = ".vlog";

pub const WAL_FILE_SUFFIX: &str = ".wal";

pub const SST_FILE_SUFFIX: &str = ".sst";

pub const MANIFEST_FILE_SUFFIX: &str = ".manifest";

pub const LOCK_FILE: &str = "LOCK";

pub fn vlog_format_path(dir: impl AsRef<Path>, fid: u32) -> PathBuf {
    dir.as_ref().join(format!("{:09}{VLOF_FILE_SUFFIX}", fid))
}

pub fn wal_format_path(dir: impl AsRef<Path>, fid: u32) -> PathBuf {
    dir.as_ref().join(format!("{:09}{WAL_FILE_SUFFIX}", fid))
}

pub fn sst_format_path(dir: impl AsRef<Path>, fid: u32) -> PathBuf {
    dir.as_ref().join(format!("{:09}{SST_FILE_SUFFIX}", fid))
}

fn get_lock_file_path(dir: impl AsRef<Path>) -> PathBuf {
    dir.as_ref().join(LOCK_FILE)
}

pub async fn lock_db(main_dir: impl AsRef<Path>, value_log_dir: impl AsRef<Path>) -> Result<()> {
    if !main_dir.as_ref().exists() {
        std::fs::create_dir_all(&main_dir)?;
    }
    if !value_log_dir.as_ref().exists() {
        std::fs::create_dir_all(&value_log_dir)?;
    }

    let main_lock = get_lock_file_path(main_dir);
    let vlog_lock = get_lock_file_path(value_log_dir);

    tracing::info!("lock db: {:?}, {:?}", main_lock, vlog_lock);

    if main_lock.exists() {
        return Error::locked(main_lock);
    }
    if vlog_lock.exists() {
        return Error::locked(vlog_lock);
    }

    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&main_lock)?;
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&vlog_lock)?;

    Ok(())
}

pub async fn unlock_db(main_dir: impl AsRef<Path>, value_log_dir: impl AsRef<Path>) -> Result<()> {
    let main_lock = get_lock_file_path(main_dir);
    let vlog_lock = get_lock_file_path(value_log_dir);

    if let Err(e) = std::fs::remove_file(&main_lock) {
        no_fail(e);
    }
    if let Err(e) = std::fs::remove_file(&vlog_lock) {
        no_fail(e);
    }

    Ok(())
}
