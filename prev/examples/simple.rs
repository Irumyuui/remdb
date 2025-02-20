use remdb::options::Options;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let mut db = Options::new().open().unwrap();

    db.put(b"1", b"2").unwrap();
    db.put(b"2", b"3").unwrap();

    println!("{:?}", db.get(b"1").unwrap());
    println!("{:?}", db.get(b"2").unwrap());

    db.close().unwrap();
}
