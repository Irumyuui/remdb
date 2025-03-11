use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use criterion::{Criterion, criterion_group, criterion_main};
use remdb_thread_pool::ThreadPool;

fn simple_task(c: &mut Criterion) {
    c.bench_function("thread_pool_simple", |b| {
        let pool = ThreadPool::new(4);
        let counter = Arc::new(AtomicUsize::new(0));

        b.iter(|| {
            let c = Arc::clone(&counter);
            pool.execute(move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
        });
    });
}

fn compute_intensive(c: &mut Criterion) {
    c.bench_function("compute_intensive", |b| {
        let pool = ThreadPool::new(8);
        let data: Vec<u64> = (0..10_000).map(|_| rand::random()).collect();

        b.iter(|| {
            let data = data.clone();
            pool.execute(move || {
                let _: u64 = data.iter().sum();
            });
        });
    });
}

fn parallel_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    for threads in [1, 2, 4, 8].iter() {
        group.bench_with_input(format!("{}_threads", threads), threads, |b, &size| {
            let pool = ThreadPool::new(size);
            let counter = Arc::new(AtomicUsize::new(0));

            b.iter(|| {
                const TASKS: usize = 1000;
                for _ in 0..TASKS {
                    let c = Arc::clone(&counter);
                    pool.execute(move || {
                        c.fetch_add(1, Ordering::SeqCst);
                    });
                }

                // 等待完成
                let start = std::time::Instant::now();
                while counter.load(Ordering::SeqCst) < TASKS
                    && start.elapsed() < Duration::from_secs(5)
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                counter.store(0, Ordering::SeqCst);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, simple_task, compute_intensive, parallel_throughput);
criterion_main!(benches);
