import argparse
import asyncio
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
import os
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse
import itertools
import re

import fire
import yaml
import httpx
from itllib.clusters import ClusterConnectionInfo

from itllib.loops import LoopConnectionInfo, ConnectionInfo, StreamConnectionInfo

DEFAULT_REMOTES = {
    "https://resources.thatone.ai": {
        "clusters": "https://clusters.thatone.ai",
        "loops": "https://streams.thatone.ai",
    },
    "https://test-resources.thatone.ai": {
        "clusters": "https://test-clusters.thatone.ai",
        "loops": "https://test-streams.thatone.ai",
    },
}


@dataclass(frozen=True)
class ResourceReference:
    kind: str
    name: str

    def __str__(self):
        return f"{self.kind}/{self.name}"


@dataclass(frozen=True)
class LocatedResource:
    path: str
    index: int
    resource: "Resource"


@dataclass(frozen=True)
class LocatedConfig:
    path: str
    index: int
    config: dict


@dataclass(frozen=True)
class ResourceId:
    kind: str
    id: str

    def __str__(self):
        return f"{self.kind}/{self.id}"


@dataclass(frozen=True)
class ResourceIdRef:
    kind: str
    id: Union[str, "DeferredValue"]

    def resolve(self, resolver: "ResourceResolver") -> ResourceId:
        if isinstance(self.id, DeferredValue):
            return ResourceId(self.kind, self.id.link(resolver))
        return ResourceId(self.kind, self.id)


@dataclass(frozen=True)
class ConfigKey:
    reference: ResourceReference
    key: str


CONFIG_KINDS = {
    "ResourceSpec",
    "Remote",
    "Client",
    "ApiKey",
    "Group",
    "Loop",
    "Stream",
    "Cluster",
}


def validate_keys(location: str, config: dict, required_keys: set, optional_keys=set()):
    """Validate that the given config has the required keys and no other keys."""
    keys = set(config.keys())
    if not keys >= required_keys:
        print(f"Error: Missing required keys in {location}: {required_keys - keys}")
        return False
    if not keys <= required_keys | optional_keys:
        print(
            f"Error: Unknown keys in {location}: {keys - required_keys - optional_keys}"
        )
        return False
    return True


CLIENT = httpx.Client()


def get_client() -> httpx.Client:
    global CLIENT
    return CLIENT


def is_itl_resource(config):
    if "apiVersion" not in config or "kind" not in config:
        return False
    if config["apiVersion"] != "thatone.ai/v1":
        return False
    if config["kind"] not in CONFIG_KINDS:
        return False

    return True


def expand_keys(config):
    if config["kind"] != "ResourceSpec":
        yield ResourceReference(config["kind"], config["metadata"]["name"])
        return

    for config_type, config_value in config["spec"].items():
        if config_type == "clients":
            for client in config_value:
                yield ResourceReference("Client", client["name"])
        elif config_type == "groups":
            for group in config_value:
                yield ResourceReference("Group", group["name"])
        elif config_type == "loops":
            for loop in config_value:
                yield ResourceReference("Loop", loop["name"])
        elif config_type == "streams":
            for stream in config_value:
                yield ResourceReference("Stream", stream["name"])
        elif config_type == "clusters":
            for cluster in config_value:
                yield ResourceReference("Cluster", cluster["name"])
        else:
            raise ValueError(f"Unknown config type {config_type}")


class DeferredValue:
    def __str__(self):
        raise NotImplementedError()

    def link(self, resolver: "ResourceResolver"):
        raise NotImplementedError()


class DeferredId(DeferredValue):
    def __init__(self, kind, name, field):
        reference = ResourceReference(kind, name)
        self.configKey = ConfigKey(reference, field)
        self._generated = None

    def link(self, resolver: "ResourceResolver"):
        if self._generated == None:
            self._generated = resolver.get_generated_id(self.configKey)
            id = ResourceId(self.configKey.reference.kind, self._generated)
            resolver.id_to_reference[id] = self.configKey.reference

        return self._generated


class ClientId(DeferredId):
    def __init__(self, name):
        super().__init__("Client", name, "clientId")


class GroupId(DeferredId):
    def __init__(self, name):
        super().__init__("Group", name, "groupId")


class LoopId(DeferredId):
    def __init__(self, name):
        super().__init__("Loop", name, "loopId")


class ClusterId(DeferredId):
    def __init__(self, name):
        super().__init__("Cluster", name, "clusterId")


class Remote(DeferredValue):
    def __init__(self, resourceKey: ResourceReference, remote=None):
        self.resource_reference = resourceKey
        self.remote = remote

    def __str__(self):
        return self.remote or str(self.resource_reference)

    def link(self, resolver: "ResourceResolver"):
        if self.remote:
            return self.remote

        resource = resolver.require_resource_by_reference(self.resource_reference)
        while True:
            # Return the remote if it's known
            remote = resource.remote
            if remote:
                if isinstance(remote, str):
                    return remote
                if isinstance(remote, Remote) and remote.remote:
                    return remote.remote

            # Otherwise, go up a level
            if resource.parent(resolver) == None:
                print("Error: Could not find parent for", resource.reference)
                raise ValueError()
            resource = resource.parent(resolver)


class Resource:
    config: dict[str, Any]
    parent_ref_or_id: Optional[tuple] = None
    kind: str
    name: str
    remote: Union[str, Remote]

    @property
    def api_version(self):
        return self.config["apiVersion"]

    def resource_ids(self) -> Generator[ResourceIdRef, None, None]:
        raise NotImplementedError()

    @property
    def reference(self):
        return ResourceReference(self.kind, self.name)

    def parent(self, resolver: "ResourceResolver"):
        if self.parent_ref_or_id == None:
            return None

        if isinstance(self.parent_ref_or_id, ResourceReference):
            return resolver.get_resource_by_reference(self.parent_ref_or_id)

        if isinstance(self.parent_ref_or_id, ResourceId):
            reference = resolver.id_to_reference[self.parent_ref_or_id]
            return resolver.get_resource_by_reference(reference)

        if isinstance(self.parent_ref_or_id, ResourceIdRef):
            parent_reference = self.parent_ref_or_id.resolve(resolver)
            reference = resolver.id_to_reference[parent_reference]
            return resolver.get_resource_by_reference(reference)

        raise ValueError(f"Invalid parent reference {self.parent_ref_or_id}")

    def get_remote(self, resolver: "ResourceResolver"):
        return resolver.link(self.remote)

    def put_with_key(self, url, json, key=None):
        client = get_client()
        if key:
            params = {"apikey": key}
        else:
            params = {}
        response = client.put(url, json=json, params=params)
        return response

    def apply(self, resolver: "ResourceResolver"):
        # Find all applicable API keys
        api_keys = resolver.apikeys_for_resource(self)

        url = resolver.link(self.remote)
        compiled_config = self.link(resolver)

        response = None
        for apikey in api_keys:
            response = self.put_with_key(
                f"{url}/{self.api_version}/{self.kind}",
                compiled_config["spec"],
                apikey.key(resolver),
            )

            if response.status_code != 401:
                break

            print(
                "Error: API key",
                apikey.key(resolver),
                "is not authorized to apply",
                self.name,
            )
            apikey.valid = False
        else:
            response = self.put_with_key(
                f"{url}/{self.api_version}/{self.kind}", compiled_config["spec"]
            )

        if response == None:
            print(f"Error: no API keys are authorized to apply {self.reference}")
            return False

        elif response.status_code != 200:
            print(
                "- Error: failed to apply",
                self.reference,
                response.status_code,
                response.text,
            )
            return False

        print("applied", self.reference)

        data = response.json()
        if data.get("apikey") != None:
            resolver.store_generated_apikey(self, data["apikey"])

        return True

    def link(self, resolver: "ResourceResolver"):
        compiled_config = self._link(self.config, resolver)
        result = {
            "apiVersion": compiled_config["apiVersion"],
            "kind": compiled_config["kind"],
            "metadata": compiled_config["metadata"],
            "spec": {x: y for x, y in compiled_config["spec"].items() if y},
        }
        return result

    def _link(self, value, resolver: "ResourceResolver"):
        if isinstance(value, DeferredValue):
            return value.link(resolver)
        elif isinstance(value, list):
            return [self._link(x, resolver) for x in value]
        elif isinstance(value, dict):
            return {k: self._link(v, resolver) for k, v in value.items()}
        elif isinstance(value, Resource):
            return value.link(resolver)
        else:
            return value


class ApiKeyResource(Resource):
    valid: bool = True

    def key(self, resolver: "ResourceResolver"):
        return resolver.link(self.config["spec"]["apiKey"])

    def namespace(self, resolver: "ResourceResolver"):
        return resolver.link(self.config["metadata"]["namespace"])

    def __init__(self, config):
        self.kind = "ApiKey"

        if "namespace" not in config["metadata"]:
            print("Error: ApiKey is missing namespace in metadata")
            raise ValueError()
        if not validate_keys("apikey", config.get("spec", {}), {"clientId", "apiKey"}):
            raise ValueError()
        self.config = config
        self.name = config["metadata"]["name"]
        self.remote = config["metadata"]["namespace"]

    def apply(self, *args, **kwargs):
        return True

    def resource_ids(self):
        return
        yield


class ClientResource(Resource):
    def __init__(self, config):
        self.kind = "Client"

        if "namespace" not in config["metadata"]:
            print("Error: Client is missing namespace in metadata")
            raise ValueError()

        if not validate_keys(
            "Client",
            config.get("spec", {}),
            {"clientId"},
            {
                "parentId",
                "dacTags",
                "dacDefaultRead",
                "dacDefaultWrite",
                "dacDefaultExecute",
                "dacWhitelistRead",
                "dacWhitelistWrite",
                "dacWhitelistExecute",
                "dacBlacklistRead",
                "dacBlacklistWrite",
                "dacBlacklistExecute",
                "macTags",
                "macWhitelistRead",
                "macWhitelistWrite",
                "macWhitelistExecute",
                "macBlacklistRead",
                "macBlacklistWrite",
                "macBlacklistExecute",
            },
        ):
            raise ValueError()
        self.config = config
        self.name = config["metadata"]["name"]
        self.remote = config["metadata"]["namespace"]

        parent_id = config["spec"].get("parentId")
        if parent_id:
            self.parent_ref_or_id = ResourceIdRef("Client", parent_id)

    def resource_ids(self):
        yield ResourceIdRef("Client", self.config["spec"]["clientId"])

    def id(self, resolver: "ResourceResolver"):
        return ResourceId("Client", resolver.link(self.config["spec"]["clientId"]))


class GroupResource(Resource):
    def __init__(self, config):
        self.kind = "Group"

        if not validate_keys(
            "Group",
            config.get("spec", {}),
            {"ownerId", "groupId", "remote"},
            {
                "public",
                "includeClients",
                "includeOrgs",
                "includeGroups",
                "excludeClients",
                "excludeOrgs",
                "excludeGroups",
            },
        ):
            raise ValueError
        self.config = config
        self.name = config["metadata"]["name"]
        self.parent_ref_or_id = ResourceIdRef("Client", config["spec"]["ownerId"])
        self.remote = config["spec"]["remote"]

    def resource_ids(self):
        yield ResourceIdRef("Group", self.config["spec"]["groupId"])


class LoopResource(Resource):
    def __init__(self, config):
        self.kind = "Loop"

        if not validate_keys(
            "loop",
            config.get("spec", {}),
            {"ownerId", "loopId", "remote"},
            {
                "dacTags",
                "dacDefaultRead",
                "dacDefaultWrite",
                "dacDefaultExecute",
                "dacWhitelistRead",
                "dacWhitelistWrite",
                "dacWhitelistExecute",
                "dacBlacklistRead",
                "dacBlacklistWrite",
                "dacBlacklistExecute",
            },
        ):
            raise ValueError()
        self.config = config
        self.name = config["metadata"]["name"]
        self.parent_ref_or_id = ResourceIdRef("Client", config["spec"]["ownerId"])
        self.remote = config["spec"]["remote"]

    def resource_ids(self) -> ResourceIdRef:
        yield ResourceIdRef("Loop", self.config["spec"]["loopId"])

    def connection_info(self, resolver: "ResourceResolver"):
        loop_id = resolver.link(self.config["spec"]["loopId"])

        base = resolver.get_remote_config(self.remote)["loops"]
        rest_base, ws_base = _split_base_uri(base)
        path = "/loop/" + loop_id

        return LoopConnectionInfo(
            rest_info=ConnectionInfo(rest_base, path),
            ws_info=ConnectionInfo(ws_base, path),
        )


def _split_base_uri(base):
    if base.startswith("unix:"):
        rest_base = f"http+unix:{base}"
        ws_base = f"ws+unix:{base}"
    elif base.startswith("http:"):
        rest_base = base
        ws_base = base.replace("http:", "ws:", 1)
    elif base.startswith("https:"):
        rest_base = base
        ws_base = base.replace("https:", "wss:", 1)
    else:
        raise ValueError(f"Invalid base URI {base}")
    return rest_base, ws_base


class StreamResource(Resource):
    def __init__(self, config):
        self.kind = "Stream"

        if not validate_keys(
            "stream",
            config.get("spec", {}),
            {"loopId", "streamId", "remote"},
            {
                "dacTags",
                "dacWhitelistRead",
                "dacWhitelistWrite",
                "dacWhitelistExecute",
                "dacBlacklistRead",
                "dacBlacklistWrite",
                "dacBlacklistExecute",
            },
        ):
            raise ValueError()
        self.config = config
        self.name = config["metadata"]["name"]
        self.parent_ref_or_id = ResourceIdRef("Loop", config["spec"]["loopId"])
        self.remote = config["spec"]["remote"]

    def resource_ids(self):
        return
        yield

    def loop(self, resolver: "ResourceResolver") -> LoopResource:
        return self.parent(resolver)

    def connection_info(self, resolver: "ResourceResolver") -> StreamConnectionInfo:
        stream_id = resolver.link(self.config["spec"]["streamId"])
        loop_info = self.loop(resolver).connection_info(resolver)
        group = None
        return loop_info.stream_connection_info(stream_id, group)


class ClusterResource(Resource):
    def __init__(self, config):
        self.kind = "Cluster"

        if not validate_keys(
            "cluster",
            config.get("spec", {}),
            {"ownerId", "clusterId", "remote"},
            {
                "dacTags",
                "dacDefaultRead",
                "dacDefaultWrite",
                "dacDefaultExecute",
                "dacWhitelistRead",
                "dacWhitelistWrite",
                "dacWhitelistExecute",
                "dacBlacklistRead",
                "dacBlacklistWrite",
                "dacBlacklistExecute",
            },
        ):
            raise ValueError()
        self.config = config
        self.name = config["metadata"]["name"]
        self.parent_ref_or_id = ResourceIdRef("Client", config["spec"]["ownerId"])
        self.remote = config["spec"]["remote"]

    def resource_ids(self):
        yield ResourceIdRef("Cluster", self.config["spec"]["clusterId"])

    def connection_info(self, resolver: "ResourceResolver") -> ClusterConnectionInfo:
        cluster_id = resolver.link(self.config["spec"]["clusterId"])

        base_uris = resolver.get_remote_config(self.remote)

        configure_base = _split_base_uri(base_uris["clusters"])[0]
        configure_path = "/cluster/" + cluster_id

        connect_base = _split_base_uri(base_uris["loops"])[1]
        connect_path = "/cluster/" + cluster_id

        return ClusterConnectionInfo(
            configure_info=ConnectionInfo(configure_base, configure_path),
            connect_info=ConnectionInfo(connect_base, connect_path),
        )


class Spec:
    def compile(self):
        raise NotImplementedError()


class ResourceSpec(Spec):
    def __init__(self, config):
        self.config = config

    def compile(self):
        """Apply the given resource config."""
        items = self.config["spec"]
        error = False

        if not validate_keys(
            "spec",
            items,
            set(),
            {"groups", "clients", "loops", "streams", "clusters"},
        ):
            error = True

        for client in items.get("clients", []):
            try:
                yield ResourceSpec.compile_client(client)
            except ValueError as e:
                error = True
                continue

        for group in items.get("groups", []):
            try:
                yield ResourceSpec.compile_group(group)
            except ValueError as e:
                error = True
                continue

        for loop in items.get("loops", []):
            try:
                yield ResourceSpec.compile_loop(loop)
            except ValueError as e:
                error = True
                continue

        for stream in items.get("streams", []):
            try:
                yield ResourceSpec.compile_stream(stream)
            except ValueError as e:
                error = True
                continue

        for cluster in items.get("clusters", []):
            try:
                yield ResourceSpec.compile_cluster(cluster)
            except ValueError as e:
                error = True
                continue

        if error:
            raise ValueError()

    @classmethod
    def compile_client(cls, config: dict):
        if not validate_keys(
            "client",
            config,
            {"name"},
            {
                "namespace",
                "parent",
                "dacTags",
                "dacDefaultRead",
                "dacDefaultWrite",
                "dacDefaultExecute",
                "dacWhitelistRead",
                "dacWhitelistWrite",
                "dacWhitelistExecute",
                "dacBlacklistRead",
                "dacBlacklistWrite",
                "dacBlacklistExecute",
                "macTags",
                "macWhitelistRead",
                "macWhitelistWrite",
                "macWhitelistExecute",
                "macBlacklistRead",
                "macBlacklistWrite",
                "macBlacklistExecute",
            },
        ):
            raise ValueError()

        client_config = {
            "apiVersion": "thatone.ai/v1",
            "kind": "Client",
            "metadata": {
                "name": config["name"],
                "namespace": Remote(
                    resourceKey=ResourceReference("Client", config["name"]),
                    remote=config.get("namespace"),
                ),
            },
            "spec": {
                "clientId": ClientId(config["name"]),
                "parentId": ClientId(config.get("parent"))
                if "parent" in config
                else None,
                "dacTags": config.get("dacTags", []),
                "dacDefaultRead": [
                    GroupId(x) for x in config.get("dacDefaultRead", [])
                ],
                "dacDefaultWrite": [
                    GroupId(x) for x in config.get("dacDefaultWrite", [])
                ],
                "dacDefaultExecute": [
                    GroupId(x) for x in config.get("dacDefaultExecute", [])
                ],
                "dacWhitelistRead": [
                    GroupId(x) for x in config.get("dacWhitelistRead", [])
                ],
                "dacWhitelistWrite": [
                    GroupId(x) for x in config.get("dacWhitelistWrite", [])
                ],
                "dacWhitelistExecute": [
                    GroupId(x) for x in config.get("dacWhitelistExecute", [])
                ],
                "dacBlacklistRead": [
                    GroupId(x) for x in config.get("dacBlacklistRead", [])
                ],
                "dacBlacklistWrite": [
                    GroupId(x) for x in config.get("dacBlacklistWrite", [])
                ],
                "dacBlacklistExecute": [
                    GroupId(x) for x in config.get("dacBlacklistExecute", [])
                ],
                "macTags": config.get("macTags", []),
                "macWhitelistRead": [
                    GroupId(x) for x in config.get("macWhitelistRead", [])
                ],
                "macWhitelistWrite": [
                    GroupId(x) for x in config.get("macWhitelistWrite", [])
                ],
                "macWhitelistExecute": [
                    GroupId(x) for x in config.get("macWhitelistExecute", [])
                ],
                "macBlacklistRead": [
                    GroupId(x) for x in config.get("macBlacklistRead", [])
                ],
                "macBlacklistWrite": [
                    GroupId(x) for x in config.get("macBlacklistWrite", [])
                ],
                "macBlacklistExecute": [
                    GroupId(x) for x in config.get("macBlacklistExecute", [])
                ],
            },
        }

        return ClientResource(client_config)

    @classmethod
    def compile_group(cls, config: dict):
        if not validate_keys(
            "group",
            config,
            {"name", "owner"},
            {
                "public",
                "includeClients",
                "includeOrgs",
                "includeGroups",
                "excludeClients",
                "excludeOrgs",
                "excludeGroups",
            },
        ):
            raise ValueError

        group_config = {
            "apiVersion": "thatone.ai/v1",
            "kind": "Group",
            "metadata": {
                "name": config["name"],
            },
            "spec": {
                "remote": Remote(
                    resourceKey=ResourceReference("Group", config["name"])
                ),
                "ownerId": ClientId(config["owner"]),
                "groupId": GroupId(config["name"]),
                "public": config.get("public", False),
                "includeClients": [
                    ClientId(x) for x in config.get("includeClients", [])
                ],
                "includeOrgs": [ClientId(x) for x in config.get("includeOrgs", [])],
                "includeGroups": [GroupId(x) for x in config.get("includeGroups", [])],
                "excludeClients": [
                    ClientId(x) for x in config.get("excludeClients", [])
                ],
                "excludeOrgs": [ClientId(x) for x in config.get("excludeOrgs", [])],
                "excludeGroups": [GroupId(x) for x in config.get("excludeGroups", [])],
            },
        }

        return GroupResource(group_config)

    @classmethod
    def compile_loop(cls, config: dict):
        if not validate_keys(
            "loop",
            config,
            {"name", "owner"},
            {
                "dacTags",
                "dacDefaultRead",
                "dacDefaultWrite",
                "dacDefaultExecute",
                "dacWhitelistRead",
                "dacWhitelistWrite",
                "dacWhitelistExecute",
                "dacBlacklistRead",
                "dacBlacklistWrite",
                "dacBlacklistExecute",
            },
        ):
            raise ValueError()

        loop_config = {
            "apiVersion": "thatone.ai/v1",
            "kind": "Loop",
            "metadata": {
                "name": config["name"],
            },
            "spec": {
                "remote": Remote(resourceKey=ResourceReference("Loop", config["name"])),
                "ownerId": ClientId(config["owner"]),
                "loopId": LoopId(config["name"]),
                "dacTags": config.get("dacTags", []),
                "dacDefaultRead": [
                    GroupId(x) for x in config.get("dacDefaultRead", [])
                ],
                "dacDefaultWrite": [
                    GroupId(x) for x in config.get("dacDefaultWrite", [])
                ],
                "dacDefaultExecute": [
                    GroupId(x) for x in config.get("dacDefaultExecute", [])
                ],
                "dacWhitelistRead": [
                    GroupId(x) for x in config.get("dacWhitelistRead", [])
                ],
                "dacWhitelistWrite": [
                    GroupId(x) for x in config.get("dacWhitelistWrite", [])
                ],
                "dacWhitelistExecute": [
                    GroupId(x) for x in config.get("dacWhitelistExecute", [])
                ],
                "dacBlacklistRead": [
                    GroupId(x) for x in config.get("dacBlacklistRead", [])
                ],
                "dacBlacklistWrite": [
                    GroupId(x) for x in config.get("dacBlacklistWrite", [])
                ],
                "dacBlacklistExecute": [
                    GroupId(x) for x in config.get("dacBlacklistExecute", [])
                ],
            },
        }

        return LoopResource(loop_config)

    @classmethod
    def compile_stream(cls, config: dict):
        if not validate_keys(
            "stream",
            config,
            {"name", "loop"},
            {
                "dacTags",
                "dacWhitelistRead",
                "dacWhitelistWrite",
                "dacWhitelistExecute",
                "dacBlacklistRead",
                "dacBlacklistWrite",
                "dacBlacklistExecute",
            },
        ):
            raise ValueError()

        stream_config = {
            "apiVersion": "thatone.ai/v1",
            "kind": "Stream",
            "metadata": {
                "name": config["name"],
            },
            "spec": {
                "remote": Remote(
                    resourceKey=ResourceReference("Stream", config["name"])
                ),
                "loopId": LoopId(config["loop"]),
                "streamId": config["name"],
                "dacTags": config.get("dacTags", []),
                "dacWhitelistRead": [
                    GroupId(x) for x in config.get("dacWhitelistRead", [])
                ],
                "dacWhitelistWrite": [
                    GroupId(x) for x in config.get("dacWhitelistWrite", [])
                ],
                "dacWhitelistExecute": [
                    GroupId(x) for x in config.get("dacWhitelistExecute", [])
                ],
                "dacBlacklistRead": [
                    GroupId(x) for x in config.get("dacBlacklistRead", [])
                ],
                "dacBlacklistWrite": [
                    GroupId(x) for x in config.get("dacBlacklistWrite", [])
                ],
                "dacBlacklistExecute": [
                    GroupId(x) for x in config.get("dacBlacklistExecute", [])
                ],
            },
        }

        return StreamResource(stream_config)

    @classmethod
    def compile_cluster(cls, config: dict):
        if not validate_keys(
            "cluster",
            config,
            {"name", "owner"},
            {
                "dacTags",
                "dacDefaultRead",
                "dacDefaultWrite",
                "dacDefaultExecute",
                "dacWhitelistRead",
                "dacWhitelistWrite",
                "dacWhitelistExecute",
                "dacBlacklistRead",
                "dacBlacklistWrite",
                "dacBlacklistExecute",
            },
        ):
            raise ValueError()

        cluster_config = {
            "apiVersion": "thatone.ai/v1",
            "kind": "Cluster",
            "metadata": {
                "name": config["name"],
            },
            "spec": {
                "remote": Remote(
                    resourceKey=ResourceReference("Cluster", config["name"])
                ),
                "ownerId": ClientId(config["owner"]),
                "clusterId": ClusterId(config["name"]),
                "dacTags": config.get("dacTags", []),
                "dacDefaultRead": [
                    GroupId(x) for x in config.get("dacDefaultRead", [])
                ],
                "dacDefaultWrite": [
                    GroupId(x) for x in config.get("dacDefaultWrite", [])
                ],
                "dacDefaultExecute": [
                    GroupId(x) for x in config.get("dacDefaultExecute", [])
                ],
                "dacWhitelistRead": [
                    GroupId(x) for x in config.get("dacWhitelistRead", [])
                ],
                "dacWhitelistWrite": [
                    GroupId(x) for x in config.get("dacWhitelistWrite", [])
                ],
                "dacWhitelistExecute": [
                    GroupId(x) for x in config.get("dacWhitelistExecute", [])
                ],
                "dacBlacklistRead": [
                    GroupId(x) for x in config.get("dacBlacklistRead", [])
                ],
                "dacBlacklistWrite": [
                    GroupId(x) for x in config.get("dacBlacklistWrite", [])
                ],
                "dacBlacklistExecute": [
                    GroupId(x) for x in config.get("dacBlacklistExecute", [])
                ],
            },
        }

        return ClusterResource(cluster_config)


class ResourcePile:
    def __init__(self, *paths: str, read_fully=False):
        self.read_fully = read_fully
        self.known_keys: dict[ResourceReference, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        self.config_sources: dict[ResourceReference, LocatedConfig] = {}
        self.compiled_resources: dict[ResourceReference, Resource] = {}

        self.add(*paths)

    def add(self, *paths: str):
        new_configs = list(
            itertools.chain(
                *[
                    self.read_resource_configs(file, read_fully=self.read_fully)
                    for file in paths
                ]
            )
        )
        new_keys = self.check_duplicates(new_configs)
        new_config_sources = self.map_config_locations(new_configs)
        new_compiled_resources = self.compile_configs(new_configs)

        self.known_keys = new_keys
        self.config_sources = new_config_sources
        self.compiled_resources = new_compiled_resources

    def read_resource_configs_file(
        self, file, read_fully=False
    ) -> Generator[LocatedConfig, None, None]:
        try:
            with open(file, "r") as f:
                configs = list(yaml.safe_load_all(f.read()))
        except Exception as e:
            print(f"Failed to read file {file}:", e.args)
            if read_fully:
                raise e
            return

        for i, config in enumerate(configs):
            if read_fully:
                yield LocatedConfig(file, i, config)
                continue

            if "apiVersion" not in config:
                print(f"Config in file {file} is missing apiVersion")
                continue
            if not isinstance(config["apiVersion"], str):
                print(
                    f"Config in file {file} has invalid apiVersion. It should be a string."
                )
                continue
            if not re.match(r"^[a-z0-9.-]+/[a-z0-9.-]+$", config["apiVersion"]):
                print(
                    f"Config in file {file} has invalid apiVersion. It should be in the format group/version. Group and version should be lowercase alphanumeric characters, dots or dashes."
                )
                continue
            if "kind" not in config:
                print(f"Config in file {file} is missing kind")
                continue
            if not isinstance(config["kind"], str):
                print(f"Config in file {file} has invalid kind. It should be a string.")
                continue
            if "metadata" not in config:
                print(f"Config in file {file} is missing metadata")
                continue
            if not isinstance(config["metadata"], dict):
                print(
                    f"Config in file {file} has invalid metadata. It should be a dict."
                )
                continue
            if "name" not in config["metadata"]:
                print(f"Config in file {file} is missing metadata.name")
                continue
            if not isinstance(config["metadata"]["name"], str):
                print(
                    f"Config in file {file} has invalid metadata.name. It should be a string."
                )
                continue

            if config["apiVersion"] != "thatone.ai/v1":
                print(
                    f"Skipping config in {file} with apiVersion {config['apiVersion']}."
                )
                continue
            if config["kind"] not in CONFIG_KINDS:
                print(f"Skipping config in {file} with kind {config['kind']}.")
                continue

            yield LocatedConfig(file, i, config)

    def read_resource_configs(self, dirpath, read_fully=False) -> List[LocatedConfig]:
        """Read the given file and return its contents."""
        dirpath = os.path.realpath(dirpath)
        if os.path.isfile(dirpath):
            return list(self.read_resource_configs_file(dirpath, read_fully=read_fully))

        result: List[LocatedConfig] = []
        for root, dirs, files in os.walk(dirpath):
            for filepath in files:
                if filepath.endswith(".yaml"):
                    fullpath = os.path.join(root, filepath)
                    result.extend(
                        self.read_resource_configs_file(fullpath, read_fully=read_fully)
                    )

        return result

    def check_duplicates(self, new_configs):
        new_keys = self.known_keys.copy()
        error = False

        for located_config in new_configs:
            if not is_itl_resource(located_config.config):
                continue

            for key in expand_keys(located_config.config):
                new_keys[key][located_config.path] += 1

        for key, file_counts in new_keys.items():
            if sum(file_counts.values()) > 1:
                error = True
                files_str = "\n  ".join(file_counts.keys())
                print(f"Error: Duplicate config {key} in files:\n  {files_str}")

        if error:
            print(
                "You can either remove each duplicate or apply the configs one at a time."
            )

        if error:
            raise ValueError()

        return new_keys

    def map_config_locations(self, prior_configs: List[LocatedConfig]):
        error = False
        new_config_sources = self.config_sources.copy()

        for located_config in prior_configs:
            if located_config.config.get("apiVersion") != "thatone.ai/v1":
                continue

            # try:
            #     resource = next(iter(self._compile_config(located_config)))
            # except ValueError as e:
            #     error = True
            #     continue

            # key = resource.reference
            key = ResourceReference(
                located_config.config["kind"], located_config.config["metadata"]["name"]
            )
            if key in new_config_sources:
                error = True
            else:
                new_config_sources[key] = located_config

        if error:
            raise ValueError()

        return new_config_sources

    def _compile_config(self, located_config: LocatedConfig) -> List["Resource"]:
        """Apply the given config."""
        kind = located_config.config["kind"]

        if kind == "ResourceSpec":
            spec = ResourceSpec(located_config.config)
            return list(spec.compile())
        elif kind == "ApiKey":
            return [ApiKeyResource(located_config.config)]
        elif kind == "Group":
            return [GroupResource(located_config.config)]
        elif kind == "Client":
            return [ClientResource(located_config.config)]
        elif kind == "Loop":
            return [LoopResource(located_config.config)]
        elif kind == "Stream":
            return [StreamResource(located_config.config)]
        elif kind == "Cluster":
            return [ClusterResource(located_config.config)]
        else:
            print("Unknown config kind", kind)
            raise ValueError(f"Unknown config kind {kind} in {located_config.path}")

    def compile_configs(self, new_configs: List[LocatedConfig]):
        error = False
        new_compiled_resources = self.compiled_resources.copy()

        for located_config in new_configs:
            try:
                generated_resources = self._compile_config(located_config)
                for resource in generated_resources:
                    key = resource.reference
                    if key in new_compiled_resources:
                        print("skipping", key, "since it was already generated")
                        continue
                    new_compiled_resources[key] = resource
            except ValueError as e:
                error = True
            except Exception as e:
                raise e

        if error:
            raise ValueError()

        return new_compiled_resources

    def link_resources(self, resolver: "ResourceResolver") -> dict[str, List[dict]]:
        unlinked_resources = self.compiled_resources
        prior_configs: Dict[
            ResourceReference, LocatedConfig
        ] = resolver.prior_resources.config_sources
        prior_resources: Dict[
            ResourceReference, Resource
        ] = resolver.prior_resources.compiled_resources

        error = False
        updated_configs: Dict[str, List[dict]] = defaultdict(list)
        updated_yaml_files = set()

        for located_config in prior_configs.values():
            updated_configs[located_config.path].append(located_config.config)

        for target_resource in unlinked_resources.values():
            reference = target_resource.reference
            if reference in prior_configs:
                location = prior_configs[reference]
                updated_configs[location.path][location.index] = target_resource.link(
                    resolver
                )
                updated_yaml_files.add(location.path)
            else:
                if target_resource.kind == "ApiKey":
                    target_file = resolver.create_secret_path(target_resource.config)
                else:
                    target_file = resolver.create_resource_path(target_resource.config)

                try:
                    updated_configs[target_file].append(target_resource.link(resolver))
                except ValueError as e:
                    print("Failed to generate config for", reference)
                    error = True
                    continue
                updated_yaml_files.add(target_file)

        if error:
            raise ValueError()

        return {x: updated_configs[x] for x in updated_yaml_files}

    def apply(
        self, prior_resources: "ResourcePile", resources_path: str, secrets_path: str
    ):
        resolver = ResourceResolver(prior_resources, self, resources_path, secrets_path)
        updated_configs = self.link_resources(resolver)

        for filepath, configs in updated_configs.items():
            dirname = os.path.dirname(filepath)
            os.makedirs(dirname, exist_ok=True)

            with open(filepath, "w") as outp:
                yaml.dump_all(
                    configs,
                    stream=outp,
                    default_flow_style=False,
                    sort_keys=False,
                    explicit_end=False,
                )

        for resource in self.compiled_resources.values():
            try:
                resource.apply(resolver)
            except Exception as e:
                print(f"Failed to apply {resource.reference}")
                print(e)
                raise

        for apikey in resolver.autogenerated_resources.values():
            path = resolver.create_secret_path(apikey.config)
            dirname = os.path.dirname(path)
            os.makedirs(dirname, exist_ok=True)
            with open(path, "w") as outp:
                yaml.dump(
                    apikey.link(resolver),
                    outp,
                    default_flow_style=False,
                    sort_keys=False,
                    explicit_end=False,
                )


class ResourceResolver:
    def __init__(
        self,
        prior_resources: ResourcePile,
        new_resources=None,
        resources_path=None,
        secrets_path=None,
    ):
        self.prior_resources: ResourcePile = prior_resources
        self.compiled_resources: dict[ResourceReference, Resource] = (
            new_resources.compiled_resources if new_resources else {}
        )
        self.autogenerated_resources: dict[ResourceReference, Resource] = {}
        self.secrets_path: Optional[str] = secrets_path
        self.resources_path: Optional[str] = resources_path
        self.generated_values: dict[ResourceReference, str] = {}
        self.id_to_reference: Dict[ResourceId, ResourceReference] = {}

        for reference, resource in self.prior_resources.compiled_resources.items():
            for resource_id in resource.resource_ids():
                self.id_to_reference[resource_id.resolve(self)] = reference

        for reference, resource in self.compiled_resources.items():
            for resource_id in resource.resource_ids():
                self.id_to_reference[resource_id.resolve(self)] = reference

    def create_path(self, folder, config):
        """Create the path for the given resource."""
        if folder == None:
            raise ValueError("No folder provided to the ResourceResolver")
        prefix = os.path.join(folder, config["kind"], config["metadata"]["name"])
        suffix = 0
        while True:
            result = prefix + (suffix or "") + ".yaml"
            if not os.path.exists(result):
                return result
            suffix += 1

    def create_secret_path(self, config):
        return self.create_path(self.secrets_path, config)

    def create_resource_path(self, config):
        return self.create_path(self.resources_path, config)

    def random_id(self):
        random_bytes = os.urandom(20)
        return "".join("{:02x}".format(byte) for byte in random_bytes)

    def get_generated_id(self, configKey: ConfigKey) -> str:
        created = (
            self.compiled_resources.get(configKey.reference)
            if self.compiled_resources
            else None
        )
        if created != None:
            spec: dict[str, Any] = created.config["spec"]
            result = spec.get(configKey.key)
            if result and not isinstance(result, DeferredValue):
                return result

        generated = self.generated_values.get(configKey.reference)
        if generated:
            return generated

        existing = self.prior_resources.compiled_resources.get(configKey.reference)
        if existing:
            result = existing.config["spec"].get(configKey.key)
            if result:
                return result

        new_id = f"{configKey.reference.name}-{self.random_id()}"
        self.generated_values[configKey.reference] = new_id
        return new_id

    def store_generated_apikey(self, cause: ClientResource, apikey_config):
        client_name = cause.config["metadata"]["name"]
        suffix = 0
        while True:
            if suffix == 0:
                candidate_name = f"{client_name}-apikey"
            else:
                candidate_name = f"{client_name}-apikey-{suffix}"

            apikey_reference = ResourceReference("ApiKey", candidate_name)
            if apikey_reference in self.compiled_resources:
                suffix += 1
                continue

            if apikey_reference in self.prior_resources.compiled_resources:
                suffix += 1
                continue

            break

        apikey = ApiKeyResource(
            {
                "apiVersion": apikey_config["apiVersion"],
                "kind": apikey_config["kind"],
                "metadata": {
                    "name": apikey_reference.name,
                    "namespace": cause.config["metadata"]["namespace"],
                },
                "spec": apikey_config["spec"],
            }
        )

        self.autogenerated_resources[apikey.reference] = apikey
        return apikey

    def link(self, id: Union[DeferredValue, ResourceId, ResourceIdRef, str]) -> str:
        if isinstance(id, str):
            return id
        elif isinstance(id, DeferredValue):
            return id.link(self)
        elif isinstance(id, ResourceId):
            return id.id
        elif isinstance(id, ResourceIdRef):
            return id.resolve(self).id
        else:
            raise ValueError(f"Invalid id {id}")

    def apikeys_for_resource(self, resource: "Resource"):
        while True:
            if isinstance(resource, ClientResource):
                resource_id = ResourceId(
                    "Client", self.link(resource.config["spec"]["clientId"])
                )
                return self.apikeys_for_client(resource_id)
            if resource.parent(self) == None:
                break
            resource = resource.parent(self)

    def get_resources_by_type(self, kind) -> Generator["Resource", None, None]:
        for candidate, resource in self.compiled_resources.items():
            if candidate.kind == kind:
                yield resource

        for candidate, value in self.prior_resources.compiled_resources.items():
            if candidate.kind == kind:
                yield value

        for candidate, value in self.autogenerated_resources.items():
            if candidate.kind == kind:
                yield value

    def get_remote_config(self, remote: Union[Remote, str]) -> "Resource":
        remote = self.link(remote)
        for candidate in self.get_resources_by_type("Remote"):
            if candidate.config["metadata"]["name"] == remote:
                return candidate.config["spec"]

        return DEFAULT_REMOTES[remote]

    def apikeys_for_client(
        self, client_id: ResourceId
    ) -> Generator["ApiKeyResource", None, None]:
        for apikey in self.get_resources_by_type("ApiKey"):
            assert isinstance(apikey, ApiKeyResource)

            if not apikey.valid:
                continue

            config = apikey.config
            if self.link(config["spec"]["clientId"]) != client_id.id:
                continue

            yield apikey

        parent_id = self.get_client_parent_id(client_id)
        if parent_id == None:
            return

        for apikey in self.apikeys_for_client(parent_id):
            yield apikey

    def get_resource_by_reference(self, reference: ResourceReference):
        created = self.compiled_resources.get(reference)
        if created:
            return created

        return self.prior_resources.compiled_resources.get(reference)

    def require_resource_by_reference(self, reference: ResourceReference):
        resource = self.get_resource_by_reference(reference)
        if resource == None:
            print("Error: Could not find resource", reference)
            raise ValueError()
        return resource

    def get_client_parent_id(self, client_id: ResourceId):
        client_reference = self.id_to_reference.get(client_id)
        if client_reference == None:
            return None

        assert client_reference is not None

        resource = self.get_resource_by_reference(client_reference)
        if resource == None:
            return None

        config = resource.config["spec"]
        if config.get("parentId") != None:
            return ResourceId("Client", self.link(config["parentId"]))

        return None
