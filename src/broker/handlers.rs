use crate::protocol::{Header, Message, MessageType, Tlv, MAGIC, VERSION};
use crate::shards::ShardedQueue;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

pub fn handle_client(mut stream: TcpStream, queue: Arc<ShardedQueue>) {
    let mut buf = [0u8; 4092];

    loop {
        match stream.read(&mut buf) {
            Ok(0) => break, // connection close
            Ok(n) => match Message::decode(&buf[..n]) {
                Ok(msg) => dispatch_message(msg, &queue, &mut stream),
                Err(e) => {
                    send_success_or_error_message(
                        &mut stream,
                        MessageType::Control,
                        "failed to decode",
                        0,
                    );
                    eprintln!("Failed to decode the message {}", e)
                }
            },
            Err(e) => {
                eprintln!("Failed to read from the client {}", e);
            }
        }
    }
}

fn dispatch_message(msg: Message, queue: &Arc<ShardedQueue>, stream: &mut TcpStream) {
    match msg.header.msg_type {
        MessageType::JobPush => handle_job_push(stream, msg, queue),
        MessageType::JobAck => handle_job_ack(stream, msg, queue),
        _ => eprintln!("Unknown message type: {:?}", msg.header.msg_type),
    }
}

fn handle_job_push(stream: &mut TcpStream, msg: Message, queue: &Arc<ShardedQueue>) {
    let key = compute_shard_key(&msg);
    queue.push(key, msg);
    send_success_or_error_message(stream, MessageType::JobAck, "success", 1);
}

fn handle_job_ack(stream: &mut TcpStream, msg: Message, queue: &Arc<ShardedQueue>) {
    let key = compute_shard_key(&msg);
    let response = queue.pop(key);
    let encoded = response.encode();
    if let Err(e) = stream.write_all(&encoded) {
        eprintln!("Failed to send ack: {}", e);
    }
}

fn compute_shard_key(msg: &Message) -> usize {
    msg.tlvs.get(0).map(|t| t.value[0] as usize).unwrap_or(0)
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
        eprintln!("Failed to send msg to the client: {}", e);
    }
}
