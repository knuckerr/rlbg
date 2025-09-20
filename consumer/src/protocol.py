import struct

MAGIC = b"RBQ1"
VERSION = 1

# Message Types
JOB_PUSH = 0x01
JOB_ACK = 0x02
CONTROL = 0x20  # for responses / errors


class Message:
    def __init__(self, msg_type, tlvs=None):
        self.msg_type = msg_type
        self.tlvs = tlvs or []

    def encode(self):
        payload = b""
        for tag, value in self.tlvs:
            length = len(value)
            payload += struct.pack(">BH", tag, length) + value
        header = (
            MAGIC
            + struct.pack(">BBH", VERSION, self.msg_type, 0)
            + struct.pack(">I", len(payload))
        )
        return header + payload

    @staticmethod
    def decode(data):
        if len(data) < 12:
            raise ValueError("Buffer too short")
        magic = data[:4]
        if magic != MAGIC:
            raise ValueError("Bad magic")
        version, msg_type = data[4], data[5]
        flags = struct.unpack(">H", data[6:8])[0]
        payload_len = struct.unpack(">I", data[8:12])[0]

        payload = data[12 : 12 + payload_len]
        tlvs = []
        i = 0
        while i < len(payload):
            tag = payload[i]
            length = struct.unpack(">H", payload[i + 1 : i + 3])[0]
            value = payload[i + 3 : i + 3 + length]
            tlvs.append((tag, value))
            i += 3 + length
        return Message(msg_type, tlvs)

    def tlvs_as_dict(self):
        """Return TLVs as a dict of tag -> value string for convenience"""
        result = {}
        for tag, value in self.tlvs:
            try:
                result[tag] = value.decode()
            except:
                result[tag] = value
        return result
