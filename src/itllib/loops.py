from dataclasses import dataclass
from typing import Optional
import boto3
import re
from urllib.parse import urlparse


@dataclass(frozen=True)
class ConnectionInfo:
    base: str
    path: str


@dataclass(frozen=True)
class LoopConnectionInfo:
    rest_info: ConnectionInfo
    ws_info: ConnectionInfo

    def stream_connection_info(self, stream_id, group=None):
        if group:
            send_path = f"{self.rest_info.path}/stream/{stream_id}/group/{group}"
            connect_path = f"{self.ws_info.path}/stream/{stream_id}/group/{group}"
        else:
            send_path = f"{self.rest_info.path}/stream/{stream_id}"
            connect_path = f"{self.ws_info.path}/stream/{stream_id}"

        return StreamConnectionInfo(
            send_info=ConnectionInfo(self.rest_info.base, send_path),
            connect_info=ConnectionInfo(self.ws_info.base, connect_path),
        )


@dataclass(frozen=True)
class StreamConnectionInfo:
    send_info: Optional[ConnectionInfo]
    connect_info: Optional[ConnectionInfo]


class LoopOperations:
    def __init__(self, connection_info: LoopConnectionInfo, apikey):
        self.connect_info = connection_info
        self.apikey = apikey

    def get_stream(self, stream_id, group=None):
        stream_connection_info = self.connect_info.stream_connection_info(
            stream_id, group
        )
        return StreamOperations(stream_connection_info, self.apikey)


class StreamOperations:
    def __init__(self, connection_info: StreamConnectionInfo, apikey):
        self.connection_info = connection_info
        self.apikey = apikey
        self.socket = None

    async def send(self, str):
        return await self.socket.send(str)

    async def recv(self):
        return await self.socket.recv()

    @property
    def connect_url(self):
        return (
            self.connection_info.connect_info.base
            + self.connection_info.connect_info.path
        )

    @property
    def send_url(self):
        return self.connection_info.send_info.base + self.connection_info.send_info.path
