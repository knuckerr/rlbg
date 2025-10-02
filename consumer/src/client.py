import asyncio
import json
import jsonschema
from typing import Dict, Optional

from .logger import logger
from .protocol import Message, JOB_PUSH, JOB_ACK, CONTROL
from .job_schema import job_schema


class Client:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader
        self.writer: asyncio.StreamWriter

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        await logger.log("INFO", f"Connected to broker {self.host}:{self.port}")

    async def validate_job_schema(self, msg: dict) -> bool:
        try:
            jsonschema.validate(instance=msg, schema=job_schema)
            return True
        except jsonschema.ValidationError as e:
            await logger.log("ERROR", f"Validation schema error {e}")
            return False

    async def push_job(self, job_id: str, payload: bytes) -> bool:
        """Send a job with arbitrary payload"""
        msg = Message(JOB_PUSH, [(0x01, job_id.encode()), (0x02, payload)])
        self.writer.write(msg.encode())
        await self.writer.drain()

        data = await self.reader.read(4096)
        if not data:
            return False

        msg = Message.decode(data)
        tlv_dict = msg.tlvs_as_dict()
        if tlv_dict.get(3) != "success":
            await logger.log("ERROR", f"Error while pushing the msg {tlv_dict}")
            return False

        return True

    async def ack_job(self, job_id: str) -> Optional[Dict]:
        """Request a job from the broker by job_id"""
        msg = Message(JOB_ACK, [(0x01, job_id.encode())])
        self.writer.write(msg.encode())
        await self.writer.drain()

        data = await self.reader.read(4096)
        if not data:
            return None

        msg = Message.decode(data)
        tlv_dict = msg.tlvs_as_dict()
        if msg.msg_type == CONTROL and tlv_dict.get(3) == "No message to pop":
            return None

        data_dict = json.loads(tlv_dict[2])
        if not await self.validate_job_schema(data_dict):
            return None

        return data_dict

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            await logger.log(
                "INFO", f"Closed connection to broker {self.host}:{self.port}"
            )
