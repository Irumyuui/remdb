#![allow(unused)]

use std::sync::Arc;

use itertools::Itertools;
use level::{LevelsController, LevelsTask};

use crate::{
    core::{Core, DBInner},
    error::Result,
    format::key::KeySlice,
    prelude::{MergeIter, TwoMergeIter},
    table::{Table, table_builder::TableBuilder, table_iter::TableConcatIter},
};

pub mod level;

impl DBInner {
    pub(crate) async fn try_compact_sstables(&self) -> Result<()> {
        let core = { self.core.read().await.clone() };
        let Some(task) = self.levels_controller.generate_task(&core) else {
            return Ok(());
        };

        todo!()
    }

    async fn do_compact(&self, task: &LevelsTask, core: &Core) -> Result<()> {
        let LevelsTask {
            upper_level,
            upper_level_ids,
            lower_level,
            lower_level_ids,
            lower_level_bottom_level,
        } = task;

        if *upper_level == 0 {
            let mut iters = Vec::with_capacity(upper_level_ids.len());
            for id in upper_level_ids {
                let iter = core.ssts_map[id].iter().await?;
                iters.push(Box::new(iter));
            }
            let upper_iter = MergeIter::new(iters).await;

            let mut lower_ssts = lower_level_ids
                .iter()
                .map(|id| core.ssts_map[id].clone())
                .collect_vec();
            let mut lower_iter = TableConcatIter::new(lower_ssts);
            lower_iter.seek_to_first().await?;

            let iter = TwoMergeIter::new(upper_iter, lower_iter).await?;
            self.do_compact_inner(iter, *lower_level_bottom_level)
                .await?;
        } else {
            let mut upper_ssts = upper_level_ids
                .iter()
                .map(|id| core.ssts_map[id].clone())
                .collect_vec();
            let mut upper_iter = TableConcatIter::new(upper_ssts);
            upper_iter.seek_to_first().await?;

            let mut lower_ssts = lower_level_ids
                .iter()
                .map(|id| core.ssts_map[id].clone())
                .collect_vec();
            let mut lower_iter = TableConcatIter::new(lower_ssts);
            lower_iter.seek_to_first().await?;

            let iter = TwoMergeIter::new(upper_iter, lower_iter).await?;
            self.do_compact_inner(iter, *lower_level_bottom_level)
                .await?;
        }

        Ok(())
    }

    async fn do_compact_inner(
        &self,
        mut iter: impl for<'a> crate::iterator::Iter<KeyType<'a> = KeySlice<'a>> + 'static, // lifetime is too hard ..
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<Table>>> {
        let mut builder = None;
        let mut result_tables = Vec::new();

        while iter.is_valid().await {
            if builder.is_none() {
                builder.replace(TableBuilder::new(self.options.clone()));
            }

            let builder_inner = builder.as_mut().unwrap();
            if compact_to_bottom_level {
                let value = iter.value().await;
                if value.meta.is_ptr() || !value.value_or_ptr.is_empty() {
                    builder_inner.add(iter.key().await.into_key_bytes(), iter.value().await);
                }
            } else {
                builder_inner.add(iter.key().await.into_key_bytes(), iter.value().await);
            }
            iter.next().await?;

            if builder_inner.current_block_count()
                > self.options.table_contains_block_count as usize
            {
                let inner = builder.take().unwrap();
                let next_table_id = self.next_table_id().await;
                let table = Arc::new(inner.finish(next_table_id).await?);
                result_tables.push(table);
            }
        }

        if let Some(inner) = builder.take() {
            let next_table_id = self.next_table_id().await;
            let table = Arc::new(inner.finish(next_table_id).await?);
            result_tables.push(table);
        }

        Ok(result_tables)
    }
}
