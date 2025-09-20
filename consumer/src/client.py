import socket
from .protocol import Message, JOB_PUSH, JOB_ACK, CONTROL
from typing import Dict, Optional

class Client:
    def __init__(self, host, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))

    def push_job(self, job_id: str, payload: bytes) -> bool:
        """Send a job with arbitrary payload"""
        msg = Message(JOB_PUSH, [(0x01, job_id.encode()), (0x02, payload)])
        self.socket.sendall(msg.encode())
        if (data := self.socket.recv(4096)):
            msg = Message.decode(data)
            tlv_dict = msg.tlvs_as_dict()
            if tlv_dict[3] != "success" :
                print(f"Error while pushing the msg {tlv_dict}")
                return False

            return True
        else:
            return False

    def ack_job(self, job_id: str) -> Optional[Dict]:
        """Request a job from the broker by job_id"""
        msg = Message(JOB_ACK, [(0x01, job_id.encode())])
        self.socket.sendall(msg.encode())
        data = self.socket.recv(4096)
        if data:
            msg = Message.decode(data)
            tlv_dict = msg.tlvs_as_dict()
            if msg.msg_type == CONTROL and tlv_dict.get(3) == "No message to pop":
                return None
            else:
                return tlv_dict

    def close(self):
        self.socket.close()
