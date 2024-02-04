import argparse
import asyncio
from contextlib import contextmanager
import os
from urllib.parse import urlparse
import requests

import fire
import yaml

from .itl import Itl
from .clusters import ClusterOperations


@contextmanager
def edit_config(config):
    if os.path.exists(config):
        with open(config, "r") as f:
            config_data = yaml.safe_load(f)
    else:
        config_data = {
            "apiVersion": "itllib/v1",
            "kind": "ItlConfig",
            "metadata": {
                "name": "itl-config",
            },
            "spec": {},
        }

    yield config_data

    with open(f"{config}.temp", "w") as f:
        yaml.dump(config_data, f)
    os.rename(f"{config}.temp", config)


def register_db_with_notifier(db, endpoint, notification_url):
    print("Setting up database notifications for", db)
    url = f"https://{endpoint}/{db}/notification"
    params = {"endpoint": notification_url + "/event"}
    headers = {"accept": "application/json"}
    response = requests.post(url, headers=headers, params=params, data="")
    print("Done")


class ConfigUpdater:
    def add_stream(self, name, loop, config="./config.yaml"):
        with edit_config(config) as config_data:
            stream_data = {
                "name": name,
                "loop": loop,
            }

            streams = config_data["spec"].setdefault("streams", [])
            for stream in streams:
                if stream["name"] != name:
                    continue

                overwrite = input(f"The stream {name} already exists. Overwrite? [y/N]")
                if overwrite.lower() != "y":
                    print(
                        f"Skipping {name}. If you want to write the config elsewhere, provide a different --config path."
                    )
                    return

                stream.clear()
                stream.update(stream_data)
                return

            streams.append(stream_data)

    def add_cluster(
        self, name, database, stream=None, config="./config.yaml", secrets="./secrets"
    ):
        itl = Itl()
        itl._apply_config(config, secrets)
        if database not in itl._old_clusters:
            raise ValueError(f"Could not find database {database} in {secrets}")

        dbOps = itl._old_clusters[database]
        if stream:
            if stream not in itl._streams:
                raise ValueError(f"Could not find stream {stream} in {secrets}")
            streamObj = itl._streams.get(stream)
        else:
            streamObj = None

        with edit_config(config) as config_data:
            cluster_data = {
                "name": name,
                "database": database,
            }

            if stream:
                cluster_data["eventStream"] = stream

            clusters = config_data["spec"].setdefault("clusters", [])
            for cluster in clusters:
                if cluster["name"] != name:
                    continue

                overwrite = input(
                    f"The cluster {name} already exists. Overwrite? [y/N]"
                )
                if overwrite.lower() != "y":
                    print(
                        f"Skipping {name}. If you want to write the config elsewhere, provide a different --config path."
                    )
                    return

                asyncio.run(ClusterOperations(dbOps, stream, streamObj, "").create())
                cluster.clear()
                cluster.update(cluster_data)
                return

            asyncio.run(ClusterOperations(dbOps, stream, streamObj, "").create())
            clusters.append(cluster_data)

    def dump_endpoints(self, config="./config.yaml", secrets="./secrets"):
        itl = Itl()
        itl._apply_config(config, secrets)

        result = {}

        for name, stream in itl._streams.items():
            result.setdefault("streams", {})[name] = {
                "post": stream.send_url,
                "websocket": stream.connect_url,
            }

        for name, db in itl._old_clusters.items():
            result.setdefault("databases", {})[name] = {
                "url": f"{db.endpoint_url}/{db.name}",
            }

        print(yaml.dump(result))


class Configurator:
    config = ConfigUpdater()

    def create_loop(self, id, endpoint, name, secrets="./secrets"):
        os.makedirs(secrets, exist_ok=True)
        secret_path = os.path.join(secrets, f"{id}.yaml")
        if os.path.exists(secret_path):
            overwrite = input(
                f"The loop config will be stored in {secret_path}. Overwrite {secret_path}? [y/N]"
            )
            if overwrite.lower() != "y":
                print(
                    f"Skipping {id}. If you want to write the config elsewhere, provide a different --secrets path."
                )
                return

        with open(secret_path, "w") as f:
            f.write(
                f"""
apiVersion: itllib/v1
kind: LoopSecret
metadata:
  name: {id}
  creationTimestamp: null
spec:
  loopName: {name}
  authenticationType: PASSWORD
  secretBasicAuth:
    endpoint: {endpoint}
    username: usernameGoesHere
    password: passwordGoesHere
  protocols: [basicAuth]
"""
            )

    def create_database(self, id, endpoint, name, notifier=None, secrets="./secrets"):
        os.makedirs(secrets, exist_ok=True)
        secret_path = os.path.join(secrets, f"{id}.yaml")
        if os.path.exists(secret_path):
            overwrite = input(
                f"The database config will be stored in {secret_path}. Overwrite {secret_path}? [y/N]"
            )
            if overwrite.lower() != "y":
                print(
                    f"Skipping {id}. If you want to write the config elsewhere, provide a different --secrets path."
                )
                return

        with open(secret_path, "w") as f:
            f.write(
                f"""
apiVersion: itllib/v1
kind: DatabaseSecret
metadata:
  name: {id}
  creationTimestamp: null
spec:
  databaseName: {name}
  notifier: {notifier}
  authenticationType: PASSWORD
  secretBasicAuth:
    endpoint: {endpoint}
    username: usernameGoesHere
    password: passwordGoesHere
  protocols: [basicAuth]
"""
            )

        if notifier:
            register_db_with_notifier(name, endpoint, notifier)

    def apply(self, config):
        raise NotImplementedError()


if __name__ == "__main__":
    fire.Fire(Configurator())
