use crate::protocol;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

pub struct Shard {
    queue: Mutex<VecDeque<protocol::Message>>,
    codvar: Condvar,
}

impl Shard {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            codvar: Condvar::new(),
        }
    }
    // push a message
    pub fn push(&self, msg: protocol::Message) {
        let mut q = self.queue.lock().unwrap();
        q.push_back(msg);
        self.codvar.notify_one();
    }

    pub fn push_batch(&self, msgs: Vec<protocol::Message>) {
        if msgs.is_empty() {
            return;
        }
        let mut q = self.queue.lock().unwrap();
        q.extend(msgs);
        self.codvar.notify_all();
    }

    pub fn pop(&self) -> protocol::Message {
        let mut q = self.queue.lock().unwrap();
        loop {
            if let Some(msg) = q.pop_front() {
                return msg;
            }
            q = self.codvar.wait(q).unwrap();
        }
    }
    pub fn pop_batch(&self, max: usize) -> Vec<protocol::Message> {
        let mut q = self.queue.lock().unwrap();
        loop {
            if !q.is_empty() {
                let mut batch: Vec<protocol::Message> = Vec::with_capacity(max.min(q.len()));
                for _ in 0..max.min(q.len()) {
                    if let Some(msg) = q.pop_front() {
                        batch.push(msg);
                    }
                }
                return batch;
            }
            q = self.codvar.wait(q).unwrap();
        }
    }
}

pub struct ShardedQueue {
    shards: Vec<Arc<Shard>>,
    shard_count: usize,
}

impl ShardedQueue {
    pub fn new(shard_count: usize) -> Self {
        let shards = (0..shard_count).map(|_| Arc::new(Shard::new())).collect();
        Self {
            shards,
            shard_count,
        }
    }

    pub fn pick_shard(&self, key: usize) -> &Arc<Shard> {
        &self.shards[key % self.shard_count]
    }

    pub fn push(&self, key: usize, msg: protocol::Message) {
        self.pick_shard(key).push(msg);
    }

    pub fn push_batch(&self, key: usize, msgs: Vec<protocol::Message>) {
        self.pick_shard(key).push_batch(msgs);
    }

    pub fn pop(&self, key: usize) -> protocol::Message {
        self.pick_shard(key).pop()
    }

    pub fn pop_batch(&self, key: usize, max: usize) -> Vec<protocol::Message> {
        self.pick_shard(key).pop_batch(max)
    }
}
