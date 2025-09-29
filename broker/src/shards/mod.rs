use crate::protocol::Message;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

const WAL_BATCH_SIZE: usize = 100;

#[derive(Debug)]
struct WalWriter {
    file: File,
    path: PathBuf,
    entries_since_flish: usize,
}

impl WalWriter {
    fn new(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
            entries_since_flish: 0,
        })
    }

    fn append(&mut self, data: &[u8]) -> io::Result<()> {
        let len = (data.len() as u32).to_le_bytes();
        self.file.write_all(&len)?;
        self.file.write_all(data)?;
        self.entries_since_flish += 1;

        if self.entries_since_flish > WAL_BATCH_SIZE {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.sync_data()?;
        self.file.seek(SeekFrom::Start(0))?;
        self.entries_since_flish = 0;
        Ok(())
    }

    fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.entries_since_flish = 0;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Shard {
    queue: Mutex<VecDeque<Message>>,
    codvar: Condvar,
}

impl Shard {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            codvar: Condvar::new(),
        }
    }

    pub fn push(&self, msg: Message) {
        let mut q = self.queue.lock().unwrap();
        q.push_back(msg);
        self.codvar.notify_one();
    }

    pub fn push_batch(&self, msgs: Vec<Message>) {
        if msgs.is_empty() {
            return;
        }
        let mut q = self.queue.lock().unwrap();
        q.extend(msgs);
        self.codvar.notify_all();
    }

    pub fn pop(&self) -> Option<Message> {
        let mut q = self.queue.lock().unwrap();
        q.pop_front()
    }
    pub fn pop_batch(&self, max: usize) -> Vec<Message> {
        let mut q = self.queue.lock().unwrap();
        let mut batch = Vec::new();
        for _ in 0..max {
            if let Some(msg) = q.pop_front() {
                batch.push(msg);
            } else {
                break;
            }
        }
        batch
    }

    pub fn try_pop(&self) -> Option<Message> {
        self.queue.lock().unwrap().pop_front()
    }

    pub fn load_snapshoot(path: &Path) -> io::Result<VecDeque<Message>> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut queue = VecDeque::new();
        let mut offset = 0;

        while offset < buffer.len() {
            if offset + 4 > buffer.len() {
                break;
            }

            let msg_len = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;

            offset += 4;

            if offset + msg_len > buffer.len() {
                break;
            }

            if let Ok(msg) = Message::decode(&buffer[offset..offset + msg_len]) {
                queue.push_back(msg);
            }

            offset += msg_len
        }

        Ok(queue)
    }

    pub fn replay_wal(path: &Path) -> io::Result<Vec<Message>> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut messages = Vec::new();
        let mut offset = 0;

        while offset < buffer.len() {
            if offset + 4 > buffer.len() {
                break;
            }
            let msg_len = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;

            offset += 4;

            if offset + msg_len > buffer.len() {
                break;
            }
            if let Ok(msg) = Message::decode(&buffer[offset..offset + msg_len]) {
                messages.push(msg);
            }
            offset += msg_len;
        }
        Ok(messages)
    }
}

#[derive(Debug)]
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

    fn pick_shard(&self, key: usize) -> &Arc<Shard> {
        &self.shards[key % self.shard_count]
    }

    pub fn push(&self, key: usize, msg: Message) {
        self.pick_shard(key).push(msg);
    }

    pub fn push_batch(&self, key: usize, msgs: Vec<Message>) {
        self.pick_shard(key).push_batch(msgs);
    }

    pub fn pop(&self, key: usize) -> Option<Message> {
        self.pick_shard(key).pop()
    }

    pub fn pop_batch(&self, key: usize, max: usize) -> Vec<Message> {
        self.pick_shard(key).pop_batch(max)
    }

    pub fn try_pop(&self, key: usize) -> Option<Message> {
        self.pick_shard(key).try_pop()
    }
}

// Global queues
static GLOBAL_QUEUE: OnceLock<Arc<ShardedQueue>> = OnceLock::new();

pub fn init_global_queue(shard_count: usize) {
    GLOBAL_QUEUE
        .set(Arc::new(ShardedQueue::new(shard_count)))
        .expect("Global queue already initialized");
}

pub fn get_global_queue() -> Arc<ShardedQueue> {
    GLOBAL_QUEUE
        .get()
        .expect("Failed to get Global queue")
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Header, Message, MessageType, Tlv, MAGIC, VERSION};
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
        let pop = queue.pop(0).unwrap();
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
                let batch: Vec<Message> = (0..10).map(|j| make_mesages(i * 10 + j)).collect();
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
