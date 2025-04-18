use bytes::Bytes;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::format::{
    key::{KeyBytes, Seq},
    value::Value,
};

#[inline]
pub fn run_async_test<F, Fut>(test_fn: F) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    init_tracing_not_failed();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(10)
        .enable_time()
        .build()?;

    rt.block_on(async { test_fn().await })?;
    Ok(())
}

#[inline]
pub fn init_tracing_not_failed() {
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_thread_ids(true)
                .with_line_number(true)
                .with_file(true),
        )
        .try_init();
    color_backtrace::install();
}

pub fn gen_key_value(seq: Seq, n: usize) -> (KeyBytes, Value) {
    let key = KeyBytes::new(Bytes::from(format!("key-{n:05}")), seq);
    let value = Value::from_raw_value(Bytes::from(format!("value-{n:05}")));
    (key, value)
}
