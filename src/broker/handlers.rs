use crate::log_error;
use crate::log_info;
use crate::logger::global_loger;
use crate::protocol::{Header, Message, MessageType, Tlv, MAGIC, VERSION};
use crate::shards::ShardedQueue;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

pub fn handle_client(mut stream: TcpStream, shard_count: usize, queue: Arc<ShardedQueue>) {
    let mut buf = [0u8; 4092];

    loop {
        match stream.read(&mut buf) {
            Ok(0) => break, // connection close
            Ok(n) => match Message::decode(&buf[..n]) {
                Ok(msg) => dispatch_message(msg, shard_count, &queue, &mut stream),
                Err(e) => {
                    send_success_or_error_message(
                        &mut stream,
                        MessageType::Control,
                        "failed to decode",
                        0,
                    );
                    log_error!(global_loger(), "Failed to decode the message {}", e);
                }
            },
            Err(e) => {
                log_error!(global_loger(), "Failed to reead from the client {}", e);
            }
        }
    }
}

fn dispatch_message(
    msg: Message,
    shard_count: usize,
    queue: &Arc<ShardedQueue>,
    stream: &mut TcpStream,
) {
    match msg.header.msg_type {
        MessageType::JobPush => handle_job_push(stream, shard_count, msg, queue),
        MessageType::JobAck => handle_job_ack(stream, shard_count, msg, queue),
        _ => {
            log_error!(
                global_loger(),
                "Unknown message type: {:?}",
                msg.header.msg_type
            );
        }
    }
}

fn handle_job_push(
    stream: &mut TcpStream,
    shard_count: usize,
    msg: Message,
    queue: &Arc<ShardedQueue>,
) {
    let key = compute_shard_key(&msg, shard_count);
    queue.push(key, msg.clone());
    log_info!(global_loger(), "Recieved {:?} ", msg);
    send_success_or_error_message(stream, MessageType::JobAck, "success", 1);
}

fn handle_job_ack(
    stream: &mut TcpStream,
    shard_count: usize,
    msg: Message,
    queue: &Arc<ShardedQueue>,
) {
    let key = compute_shard_key(&msg, shard_count);
    let response = queue.pop(key);
    match response {
        Some(msg) => {
            let encoded = msg.encode();

            if let Err(e) = stream.write_all(&encoded) {
                log_error!(global_loger(),"Failed to send ack: {}", e);
            };
        }
        None => {
            send_success_or_error_message(stream, MessageType::Control, "No message to pop", 0);
        }
    }
}

fn compute_shard_key(msg: &Message, shard_count: usize) -> usize {
    if let Some(tlv) = msg.tlvs.first() {
        let mut hash = 0usize;
        for b in &tlv.value {
            hash = hash.wrapping_mul(31).wrapping_add(*b as usize);
        }
        return hash % shard_count;
    }
    0
}

fn send_success_or_error_message(
    stream: &mut TcpStream,
    msg_type: MessageType,
    details: &str,
    flag: u16,
) {
    let msg = Message {
        header: Header {
            magic: *MAGIC,
            version: VERSION,
            msg_type: MessageType::Control,
            flags: flag,
            payload_len: 0,
        },
        tlvs: vec![
            Tlv {
                tag: 0x01,
                value: vec![flag as u8],
            },
            Tlv {
                tag: 0x02,
                value: (msg_type as u8).to_be_bytes().to_vec(),
            },
            Tlv {
                tag: 0x03,
                value: details.as_bytes().to_vec(),
            }, // error details
        ],
    };
    let encoded = msg.encode();
    if let Err(e) = stream.write_all(&encoded) {
        log_error!(global_loger(), "Failed to send msg to the client: {}", e);
    }
}
