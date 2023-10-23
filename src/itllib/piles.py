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


class BucketOperations:
    def __init__(self, secret):
        bucket_name = secret["bucketName"]
        endpoint_url = secret["secretS3"]["endpoint"]
        access_key_id = secret["secretS3"]["accessKeyID"]
        access_secret_key = secret["secretS3"]["accessSecretKey"]

        self.bucket_name = bucket_name
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=access_secret_key,
            use_ssl=False,
        )

    def put_object(self, key, file_descriptor, metadata={}):
        return self.client.put_object(
            Bucket=self.bucket_name, Key=key, Body=file_descriptor, Metadata=metadata
        )

    def get_object(self, key):
        response = self.client.get_object(Bucket=self.bucket_name, Key=key)
        contents = response["Body"]
        metadata = response["Metadata"]
        return contents, metadata

    def delete_object(self, key):
        return self.client.delete_object(Bucket=self.bucket_name, Key=key)


class PileOperations:
    def __init__(self, bucket, prefix=None):
        self.bucket = bucket
        self.prefix = prefix

    def put(self, key, file_descriptor, metadata={}):
        if self.prefix != None:
            if not key.startswith(self.prefix):
                raise ValueError(f"Key {key} needs to start with prefix {self.prefix}")

        return self.bucket.put_object(key, file_descriptor, metadata=metadata)

    def get(self, key):
        if self.prefix != None:
            if not key.startswith(self.prefix):
                raise ValueError(f"Key {key} needs to start with prefix {self.prefix}")

        contents, metadata = self.bucket.get_object(key)
        return contents, metadata

    def delete(self, key):
        if self.prefix != None:
            if not key.startswith(self.prefix):
                raise ValueError(f"Key {key} needs to start with prefix {self.prefix}")

        return self.bucket.delete_object(Key=key)
