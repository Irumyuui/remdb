use tracing_subscriber::prelude::*;

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

        let txn = db.begin_transaction().await.expect("new txn failed");
        txn.put(b"key", b"value").await.expect("put failed"); // if failed, should restart
        txn.commit().await.expect("commit failed");

        println!("get: {:?}", db.get(b"key").await.expect("get key error"));
    });
}
