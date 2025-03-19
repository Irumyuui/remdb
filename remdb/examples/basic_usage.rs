use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .worker_threads(6)
        .build()
        .expect("build tokio runtime failed");

    rt.block_on(async {
        let db = remdb::options::DBOpenOptions::new()
            .open()
            .await
            .expect("db open failed");

        let txn = db.new_txn().await.expect("new txn failed");
        txn.put(b"key", b"value").await.expect("put failed"); // if failed, should restart
        txn.commit().await.expect("commit failed");

        println!("get: {:?}", db.get(b"key").await.expect("get key error"));
    });
}
