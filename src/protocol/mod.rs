use std::io::ErrorKind;
use std::io::{Error, Result};

/*
header (12 bytes)
+--------+------+------+--------+------------+
| Magic  | Ver  | Type | Flags  | PayloadLen |
+--------+------+------+--------+------------+
| 52 42 51 31 | 01 | 01 | 00 00 | 00 00 00 13|
   "RBQ1"      1    Push  flags=0  len=19

Payload (TLV sequence)
Tag=01 Len=0005 Val= "job42"
   01 00 05 6a 6f 62 34 32

Tag=03 Len=0004 Val= [00 00 00 05]
   03 00 04 00 00 00 05

Tag=07 Len=0008 Val= [00000000670E1FA0]
   07 00 08 00 00 00 00 67 0E 1F A0

*/
pub const MAGIC: &[u8; 4] = b"RBQ1";
pub const VERSION: u8 = 1;

/// Message types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    JobPush = 0x01,
    JobAck = 0x02,
    JobResult = 0x03,
    JobStatus = 0x04,
    AiQuery = 0x10,
    AiResponse = 0x11,
    Control = 0x20,
}

impl MessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(MessageType::JobPush),
            0x02 => Some(MessageType::JobAck),
            0x03 => Some(MessageType::JobResult),
            0x04 => Some(MessageType::JobStatus),
            0x10 => Some(MessageType::AiQuery),
            0x11 => Some(MessageType::AiResponse),
            0x20 => Some(MessageType::Control),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub magic: [u8; 4],
    pub version: u8,
    pub msg_type: MessageType,
    pub flags: u16,
    pub payload_len: u32,
}

impl Header {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.magic);
        buf.push(self.version);
        buf.push(self.msg_type as u8);
        buf.extend_from_slice(&self.flags.to_be_bytes());
        buf.extend_from_slice(&self.payload_len.to_be_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < 12 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "header too short"));
        }
        let magic: [u8; 4] = buf[0..4].try_into().unwrap();
        if &magic != MAGIC {
            return Err(Error::new(ErrorKind::InvalidData, "bad magic"));
        }
        let version = buf[4];
        if version != VERSION {
            return Err(Error::new(ErrorKind::InvalidData, "wrong version"));
        }
        let msg_type = MessageType::from_u8(buf[5])
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, "unknown message type"))?;

        let flags = u16::from_be_bytes([buf[6], buf[7]]);
        let payload_len = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        Ok(Header {
            magic,
            version,
            msg_type,
            flags,
            payload_len,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Tlv {
    pub tag: u8,
    pub value: Vec<u8>,
}

impl Tlv {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let len = self.value.len() as u16;
        buf.push(self.tag);
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&self.value);
    }

    pub fn decode(mut buf: &[u8]) -> Result<Vec<Self>> {
        let mut fields = Vec::new();
        while !buf.is_empty() {
            if buf.len() < 3 {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "tlv header is too short",
                ));
            }
            let tag = buf[0];
            let len = u16::from_be_bytes([buf[1], buf[2]]) as usize;
            buf = &buf[3..];
            if buf.len() < len {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "tlv value is too short",
                ));
            }
            let value = buf[0..len].to_vec();
            fields.push(Tlv { tag, value });
            buf = &buf[len..];
        }
        Ok(fields)
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    header: Header,
    tlvs: Vec<Tlv>,
}

impl Message {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        for tlv in &self.tlvs {
            tlv.encode(&mut payload);
        }

        let mut buf = Vec::new();
        let header = Header {
            payload_len: payload.len() as u32,
            ..self.header.clone()
        };
        header.encode(&mut buf);
        buf.extend_from_slice(&payload);
        buf
    }
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < 12 {
            return Err(Error::new(ErrorKind::InvalidData, "buffer too sort"));
        }
        let header = Header::decode(&buf[0..12])?;
        if buf.len() < 12 + header.payload_len as usize {
            return Err(Error::new(ErrorKind::InvalidData, "incomplete payload"));
        }

        let payload = &buf[12..12 + header.payload_len as usize];
        let tlvs = Tlv::decode(payload)?;
        Ok(Message { header, tlvs })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_encode() {
        let header = Header {
            magic: *MAGIC,
            version: VERSION,
            msg_type: MessageType::JobAck,
            flags: 0,
            payload_len: 0,
        };

        let mut encoded_header: Vec<u8> = Vec::new();
        header.encode(&mut encoded_header);

        let expected: Vec<u8> = vec![
            b'R',
            b'B',
            b'Q',
            b'1',                      // magic
            VERSION,                   // version
            MessageType::JobAck as u8, // message type
            0x00,
            0x00, // flags (2 bytes, little endian)
            0x00,
            0x00,
            0x00,
            0x00, // payload_len (4 bytes, little endian)
        ];

        assert_eq!(
            encoded_header, expected,
            "Encoded header did not match expected bytes"
        );
    }
    #[test]
    fn test_header_decode() {
        let encoded: Vec<u8> = vec![
            b'R',
            b'B',
            b'Q',
            b'1',                      // magic
            VERSION,                   // version
            MessageType::JobAck as u8, // message type
            0x00,
            0x00, // flags (2 bytes, little endian)
            0x00,
            0x00,
            0x00,
            0x00, // payload_len (4 bytes, little endian)
        ];
        let header = Header::decode(&encoded).unwrap();
        let expected = Header {
            magic: *MAGIC,
            version: VERSION,
            msg_type: MessageType::JobAck,
            flags: 0,
            payload_len: 0,
        };
        assert_eq!(header, expected, "decoded was not matched");
    }
    #[test]
    fn test_header_decode_with_wrong_magic() {
        let encoded: Vec<u8> = vec![
            b'B',
            b'A',
            b'A',
            b'D',                      // magic
            VERSION,                   // version
            MessageType::JobAck as u8, // message type
            0x00,
            0x00, // flags (2 bytes, little endian)
            0x00,
            0x00,
            0x00,
            0x00, // payload_len (4 bytes, little endian)
        ];
        let header = Header::decode(&encoded);
        assert!(header.is_err());
    }
    #[test]
    fn test_tlv_encode() {
        let tlv = Tlv {
            tag: 1,
            value: b"hello".to_vec(),
        };
        let mut buf = Vec::new();
        tlv.encode(&mut buf);
        let expected: Vec<u8> = vec![1, 0x00, 0x05, b'h', b'e', b'l', b'l', b'o'];
        assert_eq!(buf, expected, "encoded was not matched");
    }

    #[test]
    fn test_tlv_decode() {
        let expected = vec![Tlv {
            tag: 1,
            value: b"hello".to_vec(),
        }];
        let encode: Vec<u8> = vec![1, 0x00, 0x05, b'h', b'e', b'l', b'l', b'o'];
        let decode = Tlv::decode(&encode).unwrap();
        assert_eq!(decode, expected, "encoded was not matched");
    }
    #[test]
    fn test_roundtrip_job_push() {
        let msg = Message {
            header: Header {
                magic: *MAGIC,
                version: VERSION,
                msg_type: MessageType::JobPush,
                flags: 0,
                payload_len: 0, // auto-filled in encode
            },
            tlvs: vec![
                Tlv {
                    tag: 0x01,
                    value: b"job1".to_vec(),
                },
                Tlv {
                    tag: 0x03,
                    value: 100i32.to_be_bytes().to_vec(),
                },
                Tlv {
                    tag: 0x07,
                    value: 1u64.to_be_bytes().to_vec(),
                },
            ],
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        assert_eq!(decoded.header.msg_type, MessageType::JobPush);
        assert_eq!(decoded.tlvs[0].tag, 0x01);
        assert_eq!(String::from_utf8_lossy(&decoded.tlvs[0].value), "job1");
    }
}
