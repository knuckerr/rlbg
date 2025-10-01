use crate::protocol::Message;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use crate::log_info;
use crate::logger::{global_loger};

const WAL_BATCH_SIZE: usize = 100;
const CHECKPOUNT_THRESHOLD: usize = 100;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
enum WalOp {
    Push = 1,
    Pop = 2,
}

impl WalOp {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(WalOp::Push),
            2 => Some(WalOp::Pop),
            _ => None,
        }
    }
}
impl std::fmt::Display for WalOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::Push => write!(f, "Push"),
            Self::Pop => write!(f, "Pop"),
        }
    }
}

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

    fn append(&mut self, op: WalOp, data: Option<&[u8]>) -> io::Result<()> {
        self.file.write_all(&[op as u8])?;

        if let Some(d) = data {
            let mut len = (d.len() as u32).to_le_bytes();
            self.file.write_all(&len)?;
            self.file.write_all(d)?;
            log_info!(global_loger(), "Write msg to the WalWriter with WalOp {} and len {}", op, d.len());
        } else {
            self.file.write_all(&0u32.to_le_bytes())?;
            log_info!(global_loger(), "Write msg to the WalWriter with WalOp {}", op);
        }

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
    wal: Mutex<WalWriter>,
    id: usize,
}

impl Shard {
    pub fn new(id: usize, data_dir: &Path) -> io::Result<Self> {
        let wal_path = data_dir.join(format!("shard_{}.wal", id));
        let snapshot_path = data_dir.join(format!("shard_{}.snap", id));
        let mut queue = VecDeque::new();
        if snapshot_path.exists() {
            queue = Self::load_snapshoot(&snapshot_path)?;
        }
        let mut wal = WalWriter::new(&wal_path)?;
        if wal_path.exists() {
            let wal_messages = Self::replay_wal(&wal_path)?;
            queue.extend(wal_messages);
        }

        Ok(Self {
            queue: Mutex::new(queue),
            codvar: Condvar::new(),
            wal: Mutex::new(wal),
            id,
        })
    }

    pub fn push(&self, msg: Message) {
        let encoded = msg.encode();
        {
            let mut wal = self.wal.lock().unwrap();
            let _ = wal.append(WalOp::Push, Some(&encoded));
        }
        let mut q = self.queue.lock().unwrap();
        q.push_back(msg);
        self.codvar.notify_one();
    }

    pub fn push_batch(&self, msgs: Vec<Message>) {
        if msgs.is_empty() {
            return;
        }
        {
            let mut wal = self.wal.lock().unwrap();
            for msg in &msgs {
                let encoded = msg.encode();
                let _ = wal.append(WalOp::Push, Some(&encoded));
            }
            let _ = wal.flush();
        }
        let mut q = self.queue.lock().unwrap();
        q.extend(msgs);
        self.codvar.notify_all();
    }

    pub fn pop(&self) -> Option<Message> {
        let mut q = self.queue.lock().unwrap();
        let msg = q.pop_front();
        if msg.is_some() {
            let mut wal = self.wal.lock().unwrap();
            let _ = wal.append(WalOp::Pop, None);
        }
        msg
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

    pub fn replay_wal(path: &Path) -> io::Result<VecDeque<Message>> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut queue = VecDeque::new();
        let mut offset = 0;

        while offset < buffer.len() {
            if offset + 1 > buffer.len() {
                break;
            }
            let op = WalOp::from_byte(buffer[offset]);
            offset += 1;

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

            match op {
                Some(WalOp::Push) => {
                    if offset + msg_len > buffer.len() {
                        break;
                    }
                    if let Ok(msg) = Message::decode(&buffer[offset..offset + msg_len]) {
                        queue.push_back(msg);
                    }
                    offset += msg_len;
                }
                Some(WalOp::Pop) => {
                    let _ = queue.pop_front();
                }
                None => break,
            }
        }
        Ok(queue)
    }
    pub fn checkpoint(&self, data_dir: &Path) -> io::Result<()> {
        let snapshot_path = data_dir.join(format!("shard_{}.snapshot", self.id));
        let temp_path = data_dir.join(format!("shard_{}.snap.tmp", self.id));

        let queue = self.queue.lock().unwrap();

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;

        for msg in queue.iter() {
            let encoded = msg.encode();
            let len = (encoded.len() as u32).to_le_bytes();
            file.write_all(&len)?;
            file.write_all(&encoded)?;
        }

        file.sync_all()?;
        drop(file);

        std::fs::rename(&temp_path, &snapshot_path)?;
        let mut wal = self.wal.lock().unwrap();
        wal.truncate()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ShardedQueue {
    shards: Vec<Arc<Shard>>,
    shard_count: usize,
    data_dir: PathBuf,
    checkpoint_counter: Mutex<usize>,
}

impl ShardedQueue {
    pub fn new(shard_count: usize, data_dir: impl AsRef<Path>) -> io::Result<Self> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir)?;

        let mut shards = Vec::new();
        for i in 0..shard_count {
            shards.push(Arc::new(Shard::new(i, data_dir)?));
        }
        Ok(Self {
            shards,
            shard_count,
            data_dir: data_dir.to_path_buf(),
            checkpoint_counter: Mutex::new(0),
        })
    }

    fn pick_shard(&self, key: usize) -> &Arc<Shard> {
        &self.shards[key % self.shard_count]
    }

    pub fn push(&self, key: usize, msg: Message) {
        self.pick_shard(key).push(msg);
        self.maybe_checkpoint();
    }

    pub fn push_batch(&self, key: usize, msgs: Vec<Message>) {
        self.pick_shard(key).push_batch(msgs);
        self.maybe_checkpoint();
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

    fn maybe_checkpoint(&self) {
        let mut counter = self.checkpoint_counter.lock().unwrap();
        *counter += 1;

        if *counter > CHECKPOUNT_THRESHOLD {
            *counter = 0;
            drop(counter);

            let shards = self.shards.clone();
            let data_dir = self.data_dir.clone();

            std::thread::spawn(move || {
                for shard in shards {
                    let _ = shard.checkpoint(&data_dir);
                }
            });
        }
    }

    pub fn force_checkpoint(&self) -> io::Result<()> {
        for shard in &self.shards {
            shard.checkpoint(&self.data_dir)?;
        }
        Ok(())
    }
}

// Global queues
static GLOBAL_QUEUE: OnceLock<Arc<ShardedQueue>> = OnceLock::new();

pub fn init_global_queue(shard_count: usize, data_dir: impl AsRef<Path>) -> io::Result<()> {
    let queue = Arc::new(ShardedQueue::new(shard_count, data_dir)?);
    GLOBAL_QUEUE.set(queue).map_err(|_| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "Global queue already initialized",
        )
    })
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
    use std::env;
    use std::sync::Arc;
    use std::thread;

    fn make_test_dir() -> PathBuf {
        let mut path = env::temp_dir();
        path.push(format!("rbq_test_{}", std::process::id()));
        path.push(format!(
            "{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn cleanup_test_dir(path: &Path) {
        let _ = std::fs::remove_dir_all(path);
    }

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
        let temp_dir = make_test_dir();
        let queue = ShardedQueue::new(2, &temp_dir).unwrap();
        let message = make_mesages(42);
        queue.push(0, message.clone());
        let pop = queue.pop(0).unwrap();
        assert_eq!(pop.tlvs[0].value, message.tlvs[0].value);
    }

    #[test]
    fn test_shard_push_pop_batch() {
        let temp_dir = make_test_dir();
        let queue = ShardedQueue::new(2, &temp_dir).unwrap();
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
        let temp_dir = make_test_dir();
        let queue = Arc::new(ShardedQueue::new(4, &temp_dir).unwrap());
        let mut handles: Vec<_> = vec![];

        // Producers first
        for i in 0..4 {
            let q = Arc::clone(&queue);
            handles.push(thread::spawn(move || {
                let batch: Vec<Message> = (0..10).map(|j| make_mesages(i * 10 + j)).collect();
                q.push_batch(i, batch)
            }))
        }

        // Wait for all producers to finish
        for h in handles {
            h.join().unwrap();
        }

        // Then consumers
        let mut handles: Vec<_> = vec![];
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
        cleanup_test_dir(&temp_dir);
    }
    #[test]
    fn test_wal_replay_after_push() {
        let temp_dir = make_test_dir();
        let shard_id = 0;
        let shard_path = temp_dir.join(format!("shard_{}.wal", shard_id));
        let shard = Shard::new(shard_id, &temp_dir).unwrap();

        let msg1 = make_mesages(1);
        let msg2 = make_mesages(2);

        shard.push(msg1.clone());
        shard.push(msg2.clone());

        // Force flush WAL
        shard.wal.lock().unwrap().flush().unwrap();

        // Replay WAL manually
        let replayed = Shard::replay_wal(&shard_path).unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].tlvs[0].value, msg1.tlvs[0].value);
        assert_eq!(replayed[1].tlvs[0].value, msg2.tlvs[0].value);

        cleanup_test_dir(&temp_dir);
    }

    #[test]
    fn test_wal_replay_with_pop() {
        let temp_dir = make_test_dir();
        let shard_id = 0;
        let shard_path = temp_dir.join(format!("shard_{}.wal", shard_id));
        let shard = Shard::new(shard_id, &temp_dir).unwrap();

        let msg1 = make_mesages(1);
        let msg2 = make_mesages(2);

        shard.push(msg1.clone());
        shard.push(msg2.clone());
        shard.pop(); // pop first message

        shard.wal.lock().unwrap().flush().unwrap();

        // Replay WAL manually
        let replayed_queue = Shard::replay_wal(&shard_path).unwrap();
        assert_eq!(replayed_queue.len(), 1);
        assert_eq!(replayed_queue[0].tlvs[0].value, msg2.tlvs[0].value);

        cleanup_test_dir(&temp_dir);
    }
}
