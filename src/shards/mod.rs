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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Header, Message, MessageType, Tlv, MAGIC, VERSION};
    use std::sync::Arc;
    use std::thread;

    fn make_mesages(id: usize) -> Message {
        Message {
            header: Header {
                magic: *MAGIC,
                version: VERSION,
                msg_type: MessageType::JobPush,
                flags: 0,
                payload_len: 0,
            },
            tlvs: vec![
                Tlv {
                    tag: 0x01,
                    value: format!("job{}", id).into_bytes(),
                },
                Tlv {
                    tag: 0x03,
                    value: (id as i32).to_be_bytes().to_vec(),
                },
            ],
        }
    }

    #[test]
    fn test_shard_push_pop() {
        let queue = ShardedQueue::new(2);
        let message = make_mesages(42);
        queue.push(0, message.clone());
        let pop = queue.pop(0);
        assert_eq!(pop.tlvs[0].value, message.tlvs[0].value);
    }

    #[test]
    fn test_shard_push_pop_batch() {
        let queue = ShardedQueue::new(2);
        let batch: Vec<Message> = (0..5).map(make_mesages).collect();
        queue.push_batch(1, batch.clone());
        let popped = queue.pop_batch(1, 5);
        assert_eq!(popped.len(), 5);
        for (b, p) in batch.iter().zip(popped.iter()) {
            assert_eq!(b.tlvs[0].value, p.tlvs[0].value);
        }
    }

    #[test]
    fn test_multi_threaded_producers_consumers() {
        let queue = Arc::new(ShardedQueue::new(4));
        let mut handles: Vec<_> = vec![];

        for i in 0..4 {
            let q = Arc::clone(&queue);
            handles.push(thread::spawn(move || {
                let batch: Vec<Message> = (0..10).map(|j| make_mesages(i*10 + j)).collect();
                q.push_batch(i, batch)
            }))
        }
        for i in 0..4 {
            let q = Arc::clone(&queue);
            handles.push(thread::spawn(move || {
                let batch = q.pop_batch(i, 10);
                assert_eq!(batch.len(), 10);
            }))
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}
