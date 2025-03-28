#![allow(unused)]

use std::sync::Arc;

use itertools::Itertools;
use level::{LevelsController, LevelsTask};

use crate::{
    core::{Core, DBInner},
    error::Result,
    format::key::{KeyBytes, KeySlice},
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
        mut iter: impl for<'a> crate::iterator::Iter,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<Table>>> {
        let mut builder = None;
        let mut new_tables = Vec::new();
        let mut last_key: Option<KeyBytes> = None;
        let mut first_key_below_watermark = false;
        let watermark = self.mvcc.watermark().await;

        let mut finish_builder =
            async |builder: &mut Option<TableBuilder>, done: bool| -> Result<()> {
                if builder.is_none() {
                    return Ok(());
                }

                let sst_id = self.next_table_id().await;
                let old_builder = builder.take().unwrap();
                let table = Arc::new(old_builder.finish(sst_id).await?);
                new_tables.push(table);

                if !done {
                    builder.replace(TableBuilder::new(self.options.clone()));
                }
                Ok(())
            };

        while iter.is_valid().await {
            if builder.is_none() {
                builder.replace(TableBuilder::new(self.options.clone()));
            }

            let key = iter.key().await;
            let value = iter.value().await;

            let is_same_key = last_key.as_ref().is_some_and(|k| k.key() == key.key());
            if compact_to_bottom_level
                && !is_same_key
                && key.seq() < watermark
                && value.value_or_ptr.is_empty()
            {
                last_key = Some(key.clone());
                iter.next().await?;
                first_key_below_watermark = false;
                continue;
            }

            if key.seq() < watermark {
                if is_same_key && !first_key_below_watermark {
                    iter.next().await?;
                    continue;
                }
                first_key_below_watermark = false;
            }

            let inner = builder.as_mut().expect("must have a builder");
            if inner.current_block_count() > self.options.table_contains_block_count as _
                && !is_same_key
            {
                finish_builder(&mut builder, false).await?;
            }

            if !is_same_key {
                last_key.replace(key.clone());
            }
            let inner = builder.as_mut().expect("must have a builder");
            inner.add(key, value);

            iter.next().await?;
        }

        finish_builder(&mut builder, true).await?;

        Ok(new_tables)
    }
}
