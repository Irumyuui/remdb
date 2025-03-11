#![allow(unused)] // TODO: Remove it after implementing the code

use crate::error::Result;

pub trait Iterator {
    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];

    fn is_valid(&self) -> bool;

    fn rewind(&mut self) -> Result<()>;

    fn next(&mut self) -> Result<()>;
}
