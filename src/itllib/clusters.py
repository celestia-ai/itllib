from contextlib import asynccontextmanager
import copy
from dataclasses import dataclass
import traceback
from typing import Callable
import aiohttp
import json
from urllib.parse import urlparse
from .loops import ConnectionInfo, StreamOperations


@dataclass(frozen=True)
class FiberConnectionInfo:
    connection_info_fn: Callable[[str], ConnectionInfo]
    stream_info: ConnectionInfo


@dataclass(frozen=True)
class ClusterConnectionInfo:
    cluster_id: str
    connection_info_fn: Callable[[str], ConnectionInfo]
    stream_info: ConnectionInfo

    def fiber_connection_info(self, fiber_id, group):
        return FiberConnectionInfo(
            connection_info_fn=self.connection_info_fn,
            stream_info=self.stream_info,
        )


def create_patch(old_spec, new_spec):
    if new_spec == None:
        return None

    if old_spec == None:
        return new_spec

    patch = {}
    for k, v in new_spec.items():
        if k not in old_spec:
            patch[k] = v
            continue

        if type(v) == list:
            if not all([x == y for x, y in zip(v, old_spec[k])]):
                patch[k] = v
            continue

        if type(v) == dict:
            inner_patch = create_patch(old_spec[k], v)
            if len(inner_patch) > 0:
                patch[k] = inner_patch
            continue

        if v != old_spec[k]:
            patch[k] = v
            continue

    return patch


def merge(old_spec, patch):
    if old_spec == None:
        return patch

    new_spec = old_spec.copy()
    for k, v in patch.items():
        if type(v) == dict:
            new_spec[k] = merge(old_spec[k], v)
        else:
            new_spec[k] = v
    return new_spec


def _remove_scheme(url):
    parsed = urlparse(url)
    if parsed.scheme:
        # Remove the scheme and leading // from the URL
        return parsed._replace(scheme="").geturl()[2:]
    else:
        return url


def _infer_fiber(config):
    if "metadata" in config:
        if "fiber" in config["metadata"]:
            return config["metadata"]["fiber"]
        if "optimizer" in config["metadata"]:
            return "experiment"
    return "resource"


class ClusterOperations:
    def __init__(self, connection_info: ClusterConnectionInfo, apikey):
        self.connection_info = connection_info
        self.apikey = apikey
        self.cluster_id = connection_info.cluster_id

    async def create_resource(self, config):
        name = config["metadata"]["name"]
        group, version = config["apiVersion"].split("/")
        kind = config["kind"]
        endpoint = self.connection_info.connection_info_fn(None)
        url = f"{endpoint.url}/config/{group}/{version}/{kind}"
        params = endpoint.params.copy()
        if self.apikey:
            params["apikey"] = self.apikey
        async with aiohttp.ClientSession() as session:
            # pass apikey as query parameter
            async with session.post(url, json=config, params=params) as response:
                return await response.json()

    async def read_all_resources(
        self, group, version, kind, name, fiber, utctime, cluster=None
    ):
        endpoint = self.connection_info.connection_info_fn(None)
        url = f"{endpoint.url}/config"
        params = endpoint.params.copy()
        if self.apikey:
            params["apikey"] = self.apikey
        if cluster:
            params["cluster"] = cluster
        if group:
            params["group"] = group
        if version:
            params["version"] = version
        if kind:
            params["kind"] = kind
        if name:
            params["name"] = name
        if fiber:
            params["fiber"] = fiber
        if utctime:
            params["utctime"] = utctime

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    return await response.json()
        except Exception as e:
            raise e

    async def read_resource(self, group, version, kind, name, fiber, cluster=None):
        cluster = cluster or self.cluster_id
        endpoint = self.connection_info.connection_info_fn(cluster)
        url = f"{endpoint.url}/config/{group}/{version}/{kind}/{name}"
        params = endpoint.params.copy()
        if self.apikey:
            params["apikey"] = self.apikey
        if fiber:
            params["fiber"] = fiber
        params["from_cluster"] = self.cluster_id
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                return await response.json()

    async def patch_resource(self, config):
        name = config["metadata"]["name"]
        group, version = config["apiVersion"].split("/")
        kind = config["kind"]
        endpoint = self.connection_info.connection_info_fn(None)
        url = f"{endpoint.url}/config/{group}/{version}/{kind}/{name}"
        params = endpoint.params.copy()
        if self.apikey:
            params["apikey"] = self.apikey
        async with aiohttp.ClientSession() as session:
            async with session.patch(url, json=config, params=params) as response:
                return await response.read()

    async def update_resource(self, config):
        name = config["metadata"]["name"]
        group, version = config["apiVersion"].split("/")
        kind = config["kind"]
        endpoint = self.connection_info.connection_info_fn(None)
        url = f"{endpoint.url}/config/{group}/{version}/{kind}/{name}?create=false"
        params = endpoint.params.copy()
        if self.apikey:
            params["apikey"] = self.apikey
        async with aiohttp.ClientSession() as session:
            async with session.put(url, json=config, params=params) as response:
                return await response.json()

    async def apply_resource(self, config):
        name = config["metadata"]["name"]
        group, version = config["apiVersion"].split("/")
        kind = config["kind"]
        endpoint = self.connection_info.connection_info_fn(None)
        url = f"{endpoint.url}/config/{group}/{version}/{kind}/{name}?create=true"
        params = endpoint.params.copy()
        if self.apikey:
            params["apikey"] = self.apikey
        data = json.dumps(config)
        headers = {"Content-Type": "application/json"}
        async with aiohttp.ClientSession() as session:
            async with session.put(
                url, headers=headers, data=data, params=params
            ) as response:
                text = await response.text()
                # return await response.json()
                return json.loads(text)

    async def post_resource(self, config, cluster=None):
        name = config["metadata"]["name"]
        group, version = config["apiVersion"].split("/")
        kind = config["kind"]
        endpoint = self.connection_info.connection_info_fn(cluster)
        url = f"{endpoint.url}/config/{group}/{version}/{kind}/{name}"
        params = endpoint.params.copy()
        if self.apikey:
            params["apikey"] = self.apikey
        params["from_cluster"] = self.cluster_id
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=config, params=params) as response:
                return await response.json()

    async def delete_resource(self, group, version, kind, name, fiber, cluster=None):
        endpoint = self.connection_info.connection_info_fn(cluster)
        url = f"{endpoint.url}/config/{group}/{version}/{kind}/{name}"
        params = {}
        if self.apikey:
            params["apikey"] = self.apikey
        if fiber:
            params["fiber"] = fiber
        params["from_cluster"] = self.cluster_id
        async with aiohttp.ClientSession() as session:
            async with session.delete(url, params=params) as response:
                return await response.json()

    async def read_queue(
        self,
        group=None,
        version=None,
        kind=None,
        name=None,
        fiber=None,
        utctime=None,
        cluster=None,
    ):
        endpoint = self.connection_info.connection_info_fn(None)
        url = f"{endpoint.url}/queue"
        params = endpoint.params.copy()
        if self.apikey:
            params["apikey"] = self.apikey
        if cluster:
            params["cluster"] = cluster
        if group:
            params["group"] = group
        if version:
            params["version"] = version
        if kind:
            params["kind"] = kind
        if name:
            params["name"] = name
        if fiber:
            params["fiber"] = fiber
        if utctime:
            params["timestamp"] = utctime

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                return await response.json()

    async def lock_resource(self, cluster, group, version, kind, name, fiber):
        endpoint = self.connection_info.connection_info_fn(cluster)
        url = f"{endpoint.url}/claim/{group}/{version}/{kind}/{name}"
        params = endpoint.params.copy()
        if self.apikey:
            params["apikey"] = self.apikey
        params["from_cluster"] = self.cluster_id
        if fiber:
            params["fiber"] = fiber

        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params) as response:
                return await response.json()

    async def unlock_resource(self, cluster, group, version, kind, name, fiber):
        endpoint = self.connection_info.connection_info_fn(cluster)
        url = f"{endpoint.url}/release-claim/{group}/{version}/{kind}/{name}"
        params = endpoint.params.copy()
        if self.apikey:
            params["apikey"] = self.apikey
        params["from_cluster"] = self.cluster_id
        if fiber:
            params["fiber"] = fiber

        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params) as response:
                return await response.json()

    async def resolve_resource(
        self,
        cluster,
        group,
        version,
        kind,
        name,
        fiber,
        config,
        operations,
        delete=False,
        force=False,
    ):
        endpoint = self.connection_info.connection_info_fn(cluster)
        url = f"{endpoint.url}/resolve-claim/{group}/{version}/{kind}/{name}?force={force}"
        params = endpoint.params.copy()
        if self.apikey:
            params["apikey"] = self.apikey
        params["from_cluster"] = self.cluster_id
        if fiber:
            params["fiber"] = fiber

        data = {"operations": operations, "delete": delete}
        if config != None:
            data["config"] = config

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, params=params) as response:
                return await response.json()

    def control_resource(
        self, cluster, group, version, kind, name, fiber, validate=False
    ):
        return BaseController(
            self, cluster, group, version, kind, name, fiber, validate=validate
        )


class BaseController:
    def __init__(
        self,
        config_ops: ClusterOperations,
        cluster,
        group,
        version,
        kind,
        name,
        fiber,
        validate=False,
    ):
        self.config_ops = config_ops
        self.cluster = cluster
        self.group = group
        self.version = version
        self.kind = kind
        self.name = name
        self.fiber = fiber
        self.validate = validate

        self.pending_ops = None
        self.processed_op_ids = set()
        self.observed_op_ids = set()

        self.have_current_config = False
        self.current_config = None
        self.delete_current = False
        self.locked_config_name = None
        self.config_changed = False

    async def __aenter__(self):
        await self.acquire_object()

    async def __aexit__(self, exc_type, exc, tb):
        await self.release_current_object()

    async def acquire_object(self):
        initial_ops = await self.config_ops.lock_resource(
            self.cluster,
            self.group,
            self.version,
            self.kind,
            name=self.name,
            fiber=self.fiber,
        )
        self.locked = initial_ops != []

        if initial_ops:
            self.locked_config_name = initial_ops[0]["name"]
        else:
            self.locked_config_name = None

        self.current_config = None
        self.delete_current = False
        self.have_current_config = False
        self.pending_ops = sorted(initial_ops, key=lambda x: x["timestamp"])

    async def release_current_object(self):
        if self.locked_config_name == None:
            return

        print("Warning: forcibly releasing config lock")

        await self.config_ops.resolve_resource(
            self.cluster,
            self.group,
            self.version,
            self.kind,
            self.locked_config_name,
            self.fiber,
            self.current_config if self.config_changed else None,
            list(self.processed_op_ids),
            delete=self.delete_current,
            force=True,
        )

        self.locked_config_name = None
        self.current_config = None
        self.delete_current = False
        self.have_current_config = False
        self.processed_op_ids = set()
        self.config_changed = False

    async def next_operation_batch(self):
        if self.locked_config_name == None:
            return

        force = self.observed_op_ids != self.processed_op_ids
        if force:
            print(
                "Warning: release config lock because not all operations were processed"
            )

        new_ops = await self.config_ops.resolve_resource(
            self.cluster,
            self.group,
            self.version,
            self.kind,
            self.locked_config_name,
            self.fiber,
            self.current_config if self.config_changed else None,
            list(self.processed_op_ids),
            delete=self.delete_current,
            force=force,
        )

        self.observed_op_ids = set()
        self.processed_op_ids = set()
        self.config_changed = False

        # If there are new operations for this object, queue them
        if new_ops:
            self.pending_ops = sorted(new_ops, key=lambda x: x["timestamp"])
        elif not self.name:
            # If there are no new operations for this object, check other objects if needed
            self.locked_config_name = None
            await self.acquire_object()
        else:
            self.locked_config_name = None

    def __aiter__(self):
        return self

    async def get_next_operation(self):
        if self.locked_config_name == None:
            raise StopAsyncIteration()

        if not self.pending_ops:
            await self.next_operation_batch()

        if not self.pending_ops:
            raise StopAsyncIteration()

        # Return the next operation
        result = self.pending_ops.pop(0)
        self.observed_op_ids.add(result["id"])
        result = PendingOperation(self, result)
        return result

    async def __anext__(self):
        result = await self.get_next_operation()
        if not self.validate:
            return result

        while True:
            if await result.validate():
                return result
            await result.reject()
            result = await self.get_next_operation()

    async def get_current_config(self):
        if self.have_current_config == False:
            self.current_config = await self.config_ops.read_resource(
                self.group,
                self.version,
                self.kind,
                self.locked_config_name,
                self.fiber,
                cluster=self.cluster,
            )
            self.have_current_config = True

        return copy.deepcopy(self.current_config)

    async def accept(self, pendingOp, new_config=None, delete=None):
        if new_config != None:
            if not isinstance(new_config, dict):
                raise ValueError("new_config must be a dictionary")
            metadata = new_config.get("metadata", {})
            if metadata.get("name") != self.name:
                raise ValueError(
                    "new_config must have the same name as the pending operation"
                )
            if new_config.get("apiVersion") != f"{self.group}/{self.version}":
                raise ValueError(
                    "new_config must have the same apiVersion as the pending operation"
                )
            if new_config.get("kind") != self.kind:
                raise ValueError(
                    "new_config must have the same kind as the pending operation"
                )
            if metadata.get("fiber", "resource") != self.fiber:
                raise ValueError(
                    "new_config must have the same fiber as the pending operation"
                )
            remote = metadata.get("remote")
            if remote:
                if remote != self.cluster:
                    raise ValueError(
                        "new_config must have the same cluster as the pending operation"
                    )
            else:
                if self.cluster != self.config_ops.cluster_id:
                    raise ValueError(
                        "new_config must have the same cluster as the pending operation"
                    )

        if pendingOp.data["id"] in self.processed_op_ids:
            raise ValueError(f"Operation {pendingOp.data['id']} already processed")

        self.processed_op_ids.add(pendingOp.data["id"])

        if new_config != None or pendingOp.data["operation"] != "POST":
            self.current_config = new_config or await pendingOp.new_config()
            self.config_changed = True

        if delete == None:
            self.delete_current = pendingOp.data["operation"] == "DELETE"
        else:
            self.delete_current = delete

    async def reject(self, pendingOp):
        if pendingOp.data["id"] in self.processed_op_ids:
            raise ValueError(f"Operation {pendingOp.data['id']} already processed")

        self.processed_op_ids.add(pendingOp.data["id"])


class PendingOperation:
    def __init__(self, controller: BaseController, data):
        self.controller = controller
        self.data = data
        self.cluster = self.data["cluster"]
        self.group = self.controller.group
        self.version = self.controller.version
        self.kind = self.controller.kind
        self.name = self.controller.name
        self.fiber = self.controller.fiber
        self.remote = self.controller.config_ops.cluster_id
        self.api_version = f"{self.group}/{self.version}"

    def identifier(self):
        cluster = self.cluster
        group = self.group
        version = self.version
        kind = self.kind
        name = self.name
        fiber = self.fiber

        return f"{cluster}/{group}/{version}/{kind}/{name}/{fiber}"

    async def old_config(self):
        return await self.controller.get_current_config()

    async def new_config(self):
        if self.data["operation"] == "CREATE":
            return copy.deepcopy(self.data["config"])
        elif self.data["operation"] == "DELETE":
            return None
        elif self.data["operation"] == "PATCH":
            return merge(await self.old_config(), self.data["config"])
        elif self.data["operation"] == "REPLACE":
            return copy.deepcopy(self.data["config"])
        elif self.data["operation"] == "POST":
            return await self.controller.get_current_config()

    async def message(self):
        if self.data["operation"] == "POST":
            return copy.deepcopy(self.data["config"])
        else:
            return None

    async def patch_config(self):
        return create_patch(await self.old_config(), await self.new_config())

    async def validate(self):
        if self.data["operation"] == "CREATE":
            return await self.old_config() == None
        elif self.data["operation"] == "DELETE":
            return await self.old_config() != None
        elif self.data["operation"] == "PATCH":
            return await self.old_config() != None
        elif self.data["operation"] == "REPLACE":
            if self.data["create"]:
                return True
            else:
                return await self.old_config() != None
        elif self.data["operation"] == "POST":
            return True
        else:
            return False

    async def accept(self, new_config=None, delete=None):
        await self.controller.accept(self, new_config=new_config, delete=delete)

    async def reject(self):
        await self.controller.reject(self)
