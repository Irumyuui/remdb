use std::sync::Arc;

use crate::{error::Result, format::key::KeySlice};

use super::{Table, block_iter::BlockIter};

pub struct TableIter {
    table: Arc<Table>,
    block_idx: usize,
    block_iter: Option<BlockIter>,
}

impl TableIter {
    /// create a table iter from first
    pub async fn new(table: Arc<Table>) -> Result<Self> {
        let iter = if table.block_count() > 0 {
            Some(table.read_block(0).await?.iter())
        } else {
            None
        };

        Ok(Self {
            table,
            block_idx: 0,
            block_iter: iter,
        })
    }
}

impl crate::iterator::Iter for TableIter {
    type KeyType<'a> = KeySlice<'a>;

    async fn is_valid(&self) -> bool {
        self.block_idx < self.table.block_count()
            && self.block_iter.as_ref().is_some_and(|iter| iter.is_valid())
    }

    async fn key(&self) -> Self::KeyType<'_> {
        assert!(self.is_valid().await);
        self.block_iter.as_ref().unwrap().key()
    }

    async fn value(&self) -> crate::format::value::Value {
        assert!(self.is_valid().await);
        self.block_iter.as_ref().unwrap().value()
    }

    async fn next(&mut self) -> crate::error::Result<()> {
        assert!(self.is_valid().await);

        let block_iter = self.block_iter.as_mut().unwrap();
        block_iter.next();
        if !block_iter.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.block_count() {
                self.block_iter
                    .replace(self.table.read_block(self.block_idx).await?.iter());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::{
        iterator::Iter,
        options::DBOpenOptions,
        table::table_builder::TableBuilder,
        test_utils::{gen_key_value, run_async_test},
    };

    #[test]
    fn test_iter_foreach() -> anyhow::Result<()> {
        run_async_test(async || {
            let tempdir = tempfile::tempdir()?;

            let options = DBOpenOptions::new()
                .block_size_threshold(100000)
                .enable_table_cache()
                .db_path(tempdir.path())
                .build()?;
            let mut table_builder = TableBuilder::new(options);

            const ONE_BLOCK_COUNT: usize = 3;
            const COUNT: usize = ONE_BLOCK_COUNT * 1000;

            let mut block_data = (0..COUNT).map(|n| gen_key_value(n as u64, n)).collect_vec();
            for items in block_data.chunks(ONE_BLOCK_COUNT) {
                for (key, value) in items {
                    table_builder.add(key.clone(), value.clone());
                }
                table_builder.finish_block();
            }

            let table = Arc::new(table_builder.finish(0).await?);
            assert_eq!(table.block_count(), COUNT / ONE_BLOCK_COUNT);

            let mut iter = table.iter().await?;
            let mut result = vec![];
            while iter.is_valid().await {
                result.push((iter.key().await.into_key_bytes(), iter.value().await));
                iter.next().await?;
            }

            assert_eq!(block_data.len(), result.len());
            for (excepted, actual) in block_data.iter().zip(result.iter()) {
                assert_eq!(excepted, actual);
            }

            Ok(())
        })
    }

    #[test]
    fn test_flush_one_block_and_read() -> anyhow::Result<()> {
        run_async_test(async || {
            let tempdir = tempfile::tempdir()?;

            let options = DBOpenOptions::new()
                .block_size_threshold(1000000000)
                .enable_table_cache()
                .db_path(tempdir.path())
                .build()?;
            let mut table_builder = TableBuilder::new(options);

            const COUNT: usize = 100;

            let mut block_data = (0..COUNT).map(|n| gen_key_value(n as u64, n)).collect_vec();
            for (key, value) in block_data.iter() {
                table_builder.add(key.clone(), value.clone());
            }

            let table = Arc::new(table_builder.finish(0).await?);
            assert_eq!(table.block_count(), 1);

            let mut iter = table.iter().await?;
            let mut result = vec![];
            while iter.is_valid().await {
                result.push((iter.key().await.into_key_bytes(), iter.value().await));
                iter.next().await?;
            }

            assert_eq!(block_data.len(), result.len());
            for (excepted, actual) in block_data.iter().zip(result.iter()) {
                assert_eq!(excepted, actual);
            }

            Ok(())
        })
    }
}
