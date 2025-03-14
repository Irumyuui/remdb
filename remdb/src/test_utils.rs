use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub fn run_async_test<F, Fut>(test_fn: F) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .try_init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(10)
        .build()?;

    rt.block_on(async { test_fn().await })?;
    Ok(())
}
