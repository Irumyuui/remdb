use std::mem;

use bytes::{Buf, Bytes};

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct Block {
    data: Bytes,
    restart_offset: u32,
    restart_len: u32,
}

impl Block {
    pub fn new(data: Bytes) -> Result<Self> {
        const U32_SIZE: usize = mem::size_of::<u32>();

        let len = data.len();
        if len >= U32_SIZE {
            let max_restarts_allowed = (len - U32_SIZE) / U32_SIZE;
            let restart_len = restart_len(data.as_ref());
            if restart_len as usize <= max_restarts_allowed {
                return Ok(Self {
                    data,
                    restart_offset: (len - (1 + restart_len as usize) * U32_SIZE) as u32,
                    restart_len,
                });
            }
        }

        Err(Error::Corruption("bad block data".into()))
    }
}

fn restart_len(mut data: &[u8]) -> u32 {
    data.get_u32_le()
}
