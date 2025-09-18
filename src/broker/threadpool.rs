use crate::broker::handlers::handle_client;
use crate::shards::{get_global_queue};
use std::collections::VecDeque;
use std::net::TcpStream;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

pub struct ThreadPool {
    workers: Vec<thread::JoinHandle<()>>,
    queue: Arc<(Mutex<VecDeque<TcpStream>>, Condvar)>,
}

impl ThreadPool {
    pub fn new(size: usize, shard_count: usize) -> Self {
        let queue = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
        let mut workers = Vec::with_capacity(size);

        for _ in 0..size {
            let queue_clone = Arc::clone(&queue);
            workers.push(thread::spawn(move || loop {
                let stream = {
                    let (lock, cvar) = &*queue_clone;
                    let mut q = lock.lock().unwrap();
                    while q.is_empty() {
                        q = cvar.wait(q).unwrap();
                    }
                    q.pop_front().unwrap()
                };
                let _ = std::panic::catch_unwind(|| {
                    handle_client(stream, shard_count, get_global_queue());
                });
            }));
        }

        Self { workers, queue }
    }

    pub fn submit(&self, stream: TcpStream) {
        let (lock, cvar) = &*self.queue;
        let mut q = lock.lock().unwrap();
        q.push_back(stream);
        cvar.notify_one();
    }
}
