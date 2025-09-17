use crate::broker::handlers::handle_client;
use crate::shards::{get_global_queue, init_global_queue};
use std::collections::VecDeque;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::{Condvar, Mutex};
use std::thread;

pub struct ThreadPool {
    _workers: Vec<thread::JoinHandle<()>>,
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
                handle_client(stream, shard_count, get_global_queue());
            }));
        }

        Self {
            _workers: workers,
            queue,
        }
    }
    pub fn submit(&self, stream: TcpStream) {
        let (lock, cvar) = &*self.queue;
        let mut q = lock.lock().unwrap();
        q.push_back(stream);
        cvar.notify_one();
    }
}

pub fn run(addr: &str, shard_count: usize, pool_size: usize) -> std::io::Result<()> {
    init_global_queue(shard_count);

    let listener = TcpListener::bind(addr)?;
    println!("Broker Listening on {}", addr);

    let pool = ThreadPool::new(pool_size, shard_count);

    for stream in listener.incoming() {
        match stream {
            Ok(s) => pool.submit(s),
            Err(e) => eprintln!("Failed to accept connection: {}", e),
        }
    }
    Ok(())
}
