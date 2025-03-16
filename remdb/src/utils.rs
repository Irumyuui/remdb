#![allow(unused)]

use std::{
    mem::transmute,
    path::Path,
    sync::atomic::{self, Ordering::*},
};

use bytes::Bytes;

#[inline]
pub unsafe fn from_life_slice(slice: &[u8]) -> Bytes {
    // transmute into static lifetime
    Bytes::from_static(unsafe { transmute(slice) })
}

#[inline]
pub fn sync_path(path: impl AsRef<Path>) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}
