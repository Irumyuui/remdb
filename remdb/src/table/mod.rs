#![allow(unused)]

use bloom::Bloom;
use meta_block::MetaBlock;

use crate::fs::File;

pub mod block;
pub mod bloom;
pub mod meta_block;
pub mod table_builder;

pub struct Table {
    file: File,
    table_meta: MetaBlock,
}
