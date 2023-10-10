from glob import glob
import json


class Secret:
    name: str
    data: dict

    def __init__(self, data):
        self.name = data["metadata"]["name"]
        for protocol in data["spec"]["protocols"]:
            if protocol == "s3":
                self.data["s3"] = S3SecretData(data["spec"])
            elif protocol == "http-digest":
                self.data["http-digest"] = HttpDigestSecretData(data["spec"])


class S3SecretData:
    bucket_name: str
    endpoint_url: str
    access_key_id: str
    access_secret_key: str

    def __init__(self, data):
        self.bucket_name = data["bucketName"]
        self.endpoint_url = data["secretS3"]["endpoint"]
        self.access_key_id = data["secretS3"]["accessKeyID"]
        self.access_secret_key = data["secretS3"]["accessSecretKey"]


class HttpDigestSecretData:
    url: str
    username: str
    password: str

    def __init__(self, data):
        self.url = data["url"]
        self.username = data["secretHttp"]["username"]
        self.password = data["secretHttp"]["password"]


def collect_secrets(secrets_dir):
    bucket_keys = {}
    result = {}
    for path in glob(f"{secrets_dir}/*.json"):
        with open(path) as inp:
            secret_data = json.load(inp)

        secret = Secret(secret_data)

        key_name = secret["metadata"]["name"]
        if key_name in bucket_keys:
            raise ValueError(f"Duplicate key name: {key_name}")

        protocols = secret["spec"]["protocols"]

        if "s3" in protocols:
            bucket_name = secret["spec"]["bucketName"]
            endpoint_url = secret["spec"]["secretS3"]["endpoint"]
            access_key_id = secret["spec"]["secretS3"]["accessKeyID"]
            access_secret_key = secret["spec"]["secretS3"]["accessSecretKey"]

        bucket_keys[key_name] = (
            endpoint_url,
            bucket_name,
            access_key_id,
            access_secret_key,
        )

    return bucket_keys
