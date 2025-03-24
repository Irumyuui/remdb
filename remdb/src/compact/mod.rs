#![allow(unused)]

use level::LeveledCompactionOptions;

use crate::{core::DBInner, error::Result};

pub mod level;

impl DBInner {
    pub(crate) async fn try_compact_sstables(&self) -> Result<()> {
        let core = { self.core.read().await.clone() };

        // let opts = LeveledCompactionOptions::new(

        // );

        todo!()
    }
}
