import asyncio
from typing import Any
from pydantic import BaseModel

from itllib.resources import ClusterResource


from .itl import Itl
from .clusters import BaseController, merge as apply_patch


class ResourceController:
    def __init__(self, itl: Itl, from_cluster: str, group: str, version: str, kind: str, fiber: str = 'resource'):
        self.itl: Itl = itl
        self.from_cluster = from_cluster
        self.group = group
        self.version = version
        self.kind = kind
        self.fiber = fiber
        self._parents: list[SyncedResources] = []
        self.resources: dict[str, Any] = {}
        self._started = False

    def initialize(self, *args, **kwargs):
        pass

    def start(self):
        if self._started:
            return
        self._started = True

        self.itl.controller(self.from_cluster, self.group, self.version, self.kind, fiber=self.fiber)(
            self.controller
        )

    async def controller(self, pending: BaseController):
        async for op in pending:
            message = await op.message()
            if message != None:
                old_resource = self._get_resource(pending.cluster, message["metadata"]["name"])
                try:
                    await self.post_resource(pending.cluster, message, old_resource)
                    await op.accept()
                except Exception as e:
                    await op.reject()
                    print(f"Failed to post resource {self.kind}/{message['metadata']['name']}: {e}")
                continue

            config = await op.new_config()
            if config == None:
                # Delete the resource
                resource = self._get_resource(pending.cluster, pending.name)
                try:
                    await self.delete_resource(pending.cluster, pending.group, pending.version, pending.kind, pending.name, resource)
                    self._remove_resource(pending.cluster, pending.name)
                    await op.accept()
                except Exception as e:
                    await op.reject()
                    print(f"Failed to delete resource {self.kind}/{pending.name}: {e}")
                continue

            name = config["metadata"]["name"]
            old_resource = self._get_resource(pending.cluster, name)

            try:
                if old_resource == None:
                    result = await self.create_resource(pending.cluster, config)
                    self._add_resource(pending.cluster, pending.name, result)
                else:
                    result = await self.update_resource(pending.cluster, config, old_resource)
                    self._add_resource(pending.cluster, name, result)

                await op.accept()

            except Exception as e:
                await op.reject()
                print(f"Failed to load resource {self.kind}/{pending.name}: {e}")

    async def create_resource(self, cluster, config):
        raise ValueError("create_resource not implemented")

    async def update_resource(self, cluster, config, old_resource):
        return await self.create_resource(cluster, config)

    async def post_resource(self, cluster, config, resource):
        return None

    async def delete_resource(self, cluster, group, version, kind, name, resource):
        return None

    async def put(self, config):
        if "apiVersion" not in config:
            raise ValueError("Config is missing required key: apiVersion")
        if config["apiVersion"] != f"{self.group}/{self.version}":
            raise ValueError(
                f'Config apiVersion does not match resource: {config["apiVersion"]} != {self.group}/{self.version}'
            )

        if "kind" not in config:
            raise ValueError("Config is missing required key: kind")
        if config["kind"] != self.kind:
            raise ValueError(
                f'Config kind does not match resource: {config["kind"]} != {self.kind}'
            )

        if "metadata" not in config:
            raise ValueError("Config is missing required key: metadata")
        metadata = config["metadata"]
        if not isinstance(metadata, dict):
            raise ValueError("Config metadata must be a dictionary")

        if "name" not in config["metadata"]:
            raise ValueError("Config is missing required key: metadata.name")
        name = config["metadata"]["name"]
        if not isinstance(name, str):
            raise ValueError("Config metadata.name must be a string")
        
        cluster = metadata.get("remote", self.from_cluster)

        if not name in self.resources:
            resource = await self.create_resource(cluster, config)
            self._add_resource(cluster, name, resource)
        else:
            resource = self.resources[name]
            resource = await self.update_resource(cluster, config, resource)
            self._add_resource(cluster, name, resource)

        await self.itl.cluster_apply(self.from_cluster, config)

    async def delete(self, name):
        if name in self.resources:
            resource = self.resources[name]
            await self.delete_resource(resource)
            self._remove_resource(name)
            await self.itl.cluster_delete(
                self.group, self.version, self.kind, name, cluster=self.from_cluster
            )

    async def patch(self, name, patch):
        if "apiVersion" not in patch:
            patch["apiVersion"] = f"{self.group}/{self.version}"
        else:
            if patch["apiVersion"] != f"{self.group}/{self.version}":
                raise ValueError(
                    f'Patch apiVersion does not match resource: {patch["apiVersion"]} != {self.group}/{self.version}'
                )
        if "kind" not in patch:
            patch["kind"] = self.kind
        else:
            if patch["kind"] != self.kind:
                raise ValueError(
                    f'Patch kind does not match resource: {patch["kind"]} != {self.kind}'
                )
        if "metadata" not in patch:
            patch["metadata"] = {"name": name}
        else:
            metadata = patch["metadata"]
            if not isinstance(metadata, dict):
                raise ValueError("Patch metadata must be a dictionary")
            if "name" not in metadata:
                metadata["name"] = name
            else:
                if metadata["name"] != name:
                    raise ValueError(
                        f'Patch metadata.name does not match resource name: {metadata["name"]} != {name}'
                    )
        
        cluster = metadata.get("remote", self.from_cluster)

        old_config = await self.itl.cluster_read(
            self.from_cluster, self.group, self.version, self.kind, name
        )
        if not old_config:
            raise ValueError(f"No resource found for {self.kind}/{name}")
        new_config = apply_patch(old_config, patch)

        if name not in self.resources:
            resource = await self.create_resource(cluster, new_config)
            self._add_resource(cluster, name, resource)
        else:
            resource = self.resources[name]
            resource = await self.update_resource(cluster, new_config, resource)
            self._add_resource(cluster, name, resource)

        await self.itl.cluster_patch(self.from_cluster, patch)

    async def load_existing(self):
        resources = await self.itl.cluster_read_all(
            self.from_cluster, group=self.group, version=self.version, kind=self.kind
        )
        if resources:
            for config in resources:
                cluster = config["cluster"]
                try:
                    result = await self.create_resource(cluster, config["config"])
                    
                    self._add_resource(cluster, config["name"], result)
                except Exception as e:
                    print(f'Failed to load resource {config["name"]}: {e}')

    def _register_parent(self, resource_set):
        self._parents.append(resource_set)

    def _add_resource(self, cluster, name, resource):
        self.resources[cluster, name] = resource
        key = cluster, self.group, self.version, self.kind, name
        for parent in self._parents:
            parent._collection[key] = resource

    def _get_resource(self, cluster, name):
        return self.resources.get(cluster, name)

    def _remove_resource(self, cluster, name):
        if (cluster,name) in self.resources:
            del self.resources[cluster, name]

        key = cluster, self.group, self.version, self.kind, name
        for parent in self._parents:
            if key in parent._collection:
                del parent._collection[key]


class PydanticResourceController(ResourceController):
    def __init__(
        self, resource_cls, itl: Itl, cluster: str, group: str, version: str, kind: str, fiber: str = 'resource'
    ):
        super().__init__(itl, cluster, group, version, kind, fiber=fiber)
        self.resource_cls = resource_cls

    async def create_resource(self, cluster, config):
        if "spec" not in config:
            raise ValueError("Config is missing required key: spec")
        return self.resource_cls(**config["spec"])



class SyncedResources:
    def __init__(self, itl: Itl, cluster: str, fiber: str = 'resource'):
        self.itl = itl
        self.cluster = cluster
        self.fiber = fiber
        self.cluster_id = itl._clusters[cluster].cluster_id
        self._collection = {}

    def register(self, group, version, kind):
        def decorator(controller_cls, *args, **kwargs):
            if issubclass(controller_cls, ResourceController):
                controller = controller_cls(
                    self.itl, self.cluster, group, version, kind, fiber=self.fiber
                )
            elif issubclass(controller_cls, BaseModel):
                controller = PydanticResourceController(
                    controller_cls, self.itl, self.cluster, group, version, kind, fiber=self.fiber
                )

            controller.initialize(*args, **kwargs)
            controller.start()

            self._register_controller(controller)
            self.itl.onconnect(controller.load_existing)
            return controller_cls

        return decorator

    def _register_controller(self, controller: ResourceController):
        controller._register_parent(self)
        for name, controller in controller.resources.items():
            key = (controller.group, controller.version, controller.kind, name)
            self._collection[key] = controller

    def __getitem__(self, name):
        return self._collection[name]

    def keys(self):
        return self._collection.keys()

    def values(self):
        return self._collection.values()

    def items(self):
        return self._collection.items()

    def __contains__(self, name):
        return name in self._collection

    def __iter__(self):
        return iter(self._collection)

    def __len__(self):
        return len(self._collection)

    def get(self, name, default=None):
        return self._collection.get(name, default)
