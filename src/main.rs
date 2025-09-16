mod protocol;
mod shards;

use std::sync::Arc;
use std::thread;
use std::str;
use crate::protocol::{Message, MessageType, Header, Tlv, MAGIC, VERSION};
use crate::shards::ShardedQueue;


fn main() {
    let queue = Arc::new(ShardedQueue::new(4)); // 4 shards

    // Producer: batch push
    for i in 0..2 {
        let q = Arc::clone(&queue);
        thread::spawn(move || {
            let mut batch = Vec::new();
            for j in 0..10 {
                let msg = Message {
                    header: Header {
                        magic: *MAGIC,
                        version: VERSION,
                        msg_type: MessageType::JobPush,
                        flags: 0,
                        payload_len: 0,
                    },
                    tlvs: vec![
                        Tlv { tag: 0x01, value: format!("job{}_{}", i, j).into_bytes() },
                        Tlv { tag: 0x03, value: (j as i32).to_be_bytes().to_vec() },
                    ],
                };
                batch.push(msg);
            }
            q.push_batch(i, batch);
        });
    }

    // Consumer: batch pop
    for i in 0..4 {
        let q = Arc::clone(&queue);
        thread::spawn(move || {
            let batch = q.pop_batch(i, 5); // pop up to 5 messages
            for msg in batch {
                let value = str::from_utf8(&msg.tlvs[0].value).unwrap();
                println!("Consumer {} got: {:?}", i, value);
            }
        });
    }

    thread::sleep(std::time::Duration::from_secs(2));
}
