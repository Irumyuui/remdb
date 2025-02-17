use std::{mem, sync::Arc, thread::JoinHandle};

use crossbeam::channel::{Receiver, Sender};

enum Task {
    Action(Box<dyn FnOnce() + Send + 'static>),
    Terminate,
}

pub struct ThreadPool {
    workers: Arc<Vec<Worker>>,
    sender: Sender<Task>,
}

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "ThreadPool size must be greater than 0.");

        let (sender, receiver) = crossbeam::channel::unbounded();
        let workers = Arc::new(
            (0..size)
                .map(|id| Worker::new(id, receiver.clone()))
                .collect(),
        );
        Self { workers, sender }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(Task::Action(Box::new(f)))
            .expect("Failed to send job.");
    }

    pub fn len(&self) -> usize {
        self.workers.len()
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.len() {
            self.sender
                .send(Task::Terminate)
                .expect("Failed to send termination signal.");
        }

        let workers = Arc::into_inner(mem::replace(&mut self.workers, Arc::new(Vec::new())))
            .expect("Failed to get workers.");

        for mut worker in workers {
            worker
                .thread
                .take()
                .expect("Worker thread already taken.")
                .join()
                .expect("Failed to join worker thread.");
        }
    }
}

struct Worker {
    _id: usize,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, recv: Receiver<Task>) -> Self {
        let thread = std::thread::Builder::new()
            .name(format!("worker-{}", id))
            .spawn(move || {
                Self::run_loop(id, recv);
            })
            .expect("Failed to create worker thread.");
        Self {
            _id: id,
            thread: Some(thread),
        }
    }

    fn run_loop(id: usize, recv: Receiver<Task>) {
        loop {
            match recv.recv() {
                Ok(Task::Action(action)) => {
                    tracing::debug!("Worker {} got a job; executing.", id);
                    action();
                }
                Ok(Task::Terminate) => {
                    tracing::debug!("Worker {} disconnected; shutting down.", id);
                    break;
                }
                Err(_) => {
                    tracing::error!("Worker {} disconnected; shutting down.", id);
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Barrier,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::Duration;

    #[test]
    fn test_create_pool() {
        let pool = ThreadPool::new(4);
        assert_eq!(pool.len(), 4);
    }

    #[test]
    #[should_panic(expected = "greater than 0")]
    fn test_zero_size_pool() {
        let _ = ThreadPool::new(0);
    }

    #[test]
    fn test_basic_task_execution() {
        let pool = ThreadPool::new(2);
        let counter = Arc::new(AtomicUsize::new(0));

        let c = Arc::clone(&counter);
        pool.execute(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });

        // 等待任务完成
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_concurrent_tasks() {
        let pool = ThreadPool::new(4);
        let counter = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(4));

        for _ in 0..100 {
            let c = Arc::clone(&counter);
            let b = Arc::clone(&barrier);
            pool.execute(move || {
                b.wait();
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        // 等待所有任务完成
        while counter.load(Ordering::SeqCst) < 100 {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }

    #[test]
    fn test_termination() {
        let pool = ThreadPool::new(2);
        let terminated = Arc::new(AtomicBool::new(false));

        let t = Arc::clone(&terminated);
        pool.execute(move || {
            std::thread::sleep(Duration::from_millis(50));
            t.store(true, Ordering::SeqCst);
        });

        // 立即drop pool
        drop(pool);

        assert!(terminated.load(Ordering::SeqCst));
    }

    #[test]
    fn test_heavy_load() {
        let pool = ThreadPool::new(8);
        let counter = Arc::new(AtomicUsize::new(0));

        const TASKS: usize = 10_000;
        for _ in 0..TASKS {
            let c = Arc::clone(&counter);
            pool.execute(move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        // 等待所有任务完成
        let start = std::time::Instant::now();
        while counter.load(Ordering::SeqCst) < TASKS && start.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(counter.load(Ordering::SeqCst), TASKS);
    }

    #[test]
    fn test_mixed_workload() {
        let pool = ThreadPool::new(4);
        let results = Arc::new(std::sync::Mutex::new(Vec::new()));

        // CPU密集型任务
        for i in 0..100 {
            let r = Arc::clone(&results);
            pool.execute(move || {
                let _ = (0..1000).sum::<usize>(); // 模拟计算
                r.lock().unwrap().push(i);
            });
        }

        // IO密集型任务
        for _ in 0..100 {
            let r = Arc::clone(&results);
            pool.execute(move || {
                std::thread::sleep(Duration::from_micros(100));
                r.lock().unwrap().push(999);
            });
        }

        // 等待任务完成
        let start = std::time::Instant::now();
        while results.lock().unwrap().len() < 200 && start.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(results.lock().unwrap().len(), 200);
    }
}
