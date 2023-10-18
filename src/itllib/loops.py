import boto3
import re
from urllib.parse import urlparse


def split_s3_url(url):
    # Parse the URL
    parsed_url = urlparse(url)

    # Get the netloc (hostname and port)
    netloc = parsed_url.netloc

    # Remove the port number from the netloc
    hostname = netloc.split(":")[0]
    port = netloc.split(":")[1]

    # Construct the endpoint URL
    endpoint_url = f"{parsed_url.scheme}://{hostname}:{port}"

    # Split the path to retrieve the bucket name and object key
    path_parts = parsed_url.path.lstrip("/").split("/", 1)
    bucket_name = path_parts[0]
    object_key = path_parts[1] if len(path_parts) > 1 else None

    return endpoint_url, bucket_name, object_key


class LoopOperations:
    def __init__(self, secret):
        loop_name = secret["loopName"]
        self.endpoint_url = secret["secretBasicAuth"]["endpoint"]
        auth_username = secret["secretBasicAuth"]["username"]
        auth_password = secret["secretBasicAuth"]["password"]
        self.loop_name = loop_name

    @property
    def connect_url(self):
        return f"ws://{self.endpoint_url}/loop/{self.loop_name}/connect"

    @property
    def send_url(self):
        return f"http://{self.endpoint_url}/loop/{self.loop_name}/send"


class StreamOperations:
    def __init__(self, loop, key, group=None):
        self.loop = loop
        self.key = key
        self.group = group
        self.socket = None

    async def send(self, str):
        return await self.socket.send(str)

    async def recv(self):
        return await self.socket.recv()

    @property
    def connect_url(self):
        if self.group != None:
            return f"{self.loop.connect_url}/{self.key}/{self.group}"
        else:
            return f"{self.loop.connect_url}/{self.key}"

    @property
    def send_url(self):
        return f"{self.loop.send_url}/{self.key}"
