use crate::broker::handlers::handle_client;
use crate::shards::get_global_queue;
use std::collections::VecDeque;
use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

pub struct ThreadPool {
    workers: Vec<thread::JoinHandle<()>>,
    queue: Arc<(Mutex<QueueState>, Condvar, Condvar)>, // (lock, task_cvar, space_cvar)
    is_shutdown: Arc<AtomicBool>,
    max_queue_size: usize,
}

struct QueueState {
    tasks: VecDeque<TcpStream>,
}

impl ThreadPool {
    pub fn new(size: usize, shard_count: usize, max_queue_size: usize) -> Self {
        let queue = Arc::new((
            Mutex::new(QueueState {
                tasks: VecDeque::new(),
            }),
            Condvar::new(), // task available
            Condvar::new(), // space available
        ));
        let is_shutdown = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(size);

        for _ in 0..size {
            let queue_clone = Arc::clone(&queue);
            let shutdown_clone = Arc::clone(&is_shutdown);

            workers.push(thread::spawn(move || loop {
                let stream_opt = {
                    let (lock, task_cvar, space_cvar) = &*queue_clone;
                    let mut state = lock.lock().unwrap();
                    while state.tasks.is_empty() && !shutdown_clone.load(Ordering::Acquire) {
                        state = task_cvar.wait(state).unwrap();
                    }
                    if shutdown_clone.load(Ordering::Acquire) && state.tasks.is_empty() {
                        return;
                    }
                    let stream = state.tasks.pop_front();
                    space_cvar.notify_one();
                    stream
                };

                if let Some(stream) = stream_opt {
                    let result = std::panic::catch_unwind(|| {
                        handle_client(stream, shard_count, get_global_queue());
                    });
                    if let Err(e) = result {
                        eprintln!("Worker panic {:?}", e);
                    }
                }
            }));
        }
        Self {
            workers,
            queue,
            is_shutdown,
            max_queue_size,
        }
    }

    pub fn submit(&self, stream: TcpStream) -> Result<(), Error> {
        if self.is_shutdown.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorKind::ConnectionRefused,
                "broker is shutdown",
            ));
        }
        let (lock, task_cvar, space_cvar) = &*self.queue;
        let mut state = lock.lock().unwrap();
        if state.tasks.len() > self.max_queue_size {
            eprintln!("Queue is full");
            return Err(Error::new(ErrorKind::StorageFull, "Queue is full"));
        }
        state.tasks.push_back(stream);
        task_cvar.notify_one();
        Ok(())
    }

    pub fn shutdown(&mut self) {
        self.initiate_shutdown();
        self.join_workers();
    }

    pub fn initiate_shutdown(&self) {
        self.is_shutdown.store(true, Ordering::Release);

        let (lock, task_cvar, space_cvar) = &*self.queue;
        let _unused = lock.lock().unwrap();
        task_cvar.notify_all();
        space_cvar.notify_all();
    }

    fn join_workers(&mut self) {
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        if !self.is_shutdown.load(Ordering::Acquire) {
            self.initiate_shutdown();
        }
        self.join_workers();
    }
}
