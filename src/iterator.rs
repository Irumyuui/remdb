#![allow(unused)] // TODO: Remove it after implementing the code

use crate::error::Result;

pub trait Iterator {
    fn is_valid(&self) -> bool;

    fn status(&mut self) -> Result<()>;

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];

    fn prev(&mut self);

    fn next(&mut self);

    fn seek(&mut self, target: &[u8]);

    fn seek_to_first(&mut self);

    fn seek_to_last(&mut self);
}
