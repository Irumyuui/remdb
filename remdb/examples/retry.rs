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

        let txn1 = db.begin_transaction().await.expect("new txn1 failed");
        println!("txn1 reads key1: {:?}", txn1.get(b"key1").await);

        let txn2 = db.begin_transaction().await.expect("new txn2 failed");
        txn2.put(b"key1", b"value").await.expect("txn2 put error");
        txn2.commit().await.expect("txn2 commit error");

        txn1.put(b"key1", b"value2").await.expect("txn1 put error");

        // println!("{:?}", txn1.commit().await);
        match txn1.commit().await {
            Ok(_) => println!("txn1 commit success"),
            Err(e) => println!("txn1 commit failed, reason: {}", e),
        }
    });
}
