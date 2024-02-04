import asyncio
from typing import Any
from pydantic import BaseModel


from .itl import Itl
from .clusters import BaseController


class ClusterController:
    def __init__(self, itl: Itl, cluster: str, group: str, version: str, kind: str):
        self.itl = itl
        self.cluster = cluster
        self.group = group
        self.version = version
        self.kind = kind
        self._parents: list[SyncedResources] = []
        self.resources: dict[str, Any] = {}

        print(
            "Loaded resource controller for",
            cluster,
            self.group,
            self.version,
            kind,
        )
        itl.controller(cluster, self.group, self.version, kind)(self.controller)

    async def controller(self, pending: BaseController):
        async for op in pending:
            config = await op.new_config()
            if config == None:
                # Delete the resource
                resource = self._get_resource(pending.name)
                await self.delete_resource(resource)
                self._remove_resource(pending.name)
                await op.accept()
                print("Deleted", f"{self.kind}/{pending.name}")
                continue

            name = config["metadata"]["name"]
            old_resource = self._get_resource(name)

            try:
                if old_resource == None:
                    result = await self.create_resource(config)
                    if result == None:
                        await op.reject()
                        print("Rejected", f"{self.kind}/{pending.name}")
                        continue

                    self._add_resource(pending.name, result)
                    print("Created", f"{self.kind}/{pending.name}")
                else:
                    result = await self.update_resource(old_resource, config)
                    self._add_resource(name, result)
                    print("Reconfigured", f"{self.kind}/{pending.name}")

                await op.accept()

            except Exception as e:
                await op.reject()
                print(f"Failed to load resource {self.kind}/{pending.name}: {e}")

    async def create_resource(self, config):
        raise ValueError("create_resource not implemented")

    async def update_resource(self, resource, config):
        result = await self.create_resource(config)
        if result == None:
            raise ValueError("create_resource returned None for", config)
        return result

    async def delete_resource(self, resource):
        pass

    async def load_existing(self):
        resources = await self.itl.cluster_read_all(
            self.cluster, self.group, self.version, self.kind
        )
        if resources:
            for config in resources:
                try:
                    result = await self.create_resource(config["config"])
                    self._add_resource(config["name"], result)
                    print("Loaded", self.kind, config["name"])
                except Exception as e:
                    print(f'Failed to load resource {config["name"]}: {e}')

    def _register_parent(self, resource_set):
        self._parents.append(resource_set)

    def _add_resource(self, name, resource):
        self.resources[name] = resource
        key = self.group + "/" + self.version + "/" + self.kind + "/" + name
        for parent in self._parents:
            parent._collection[key] = resource

    def _get_resource(self, name):
        return self.resources.get(name)

    def _remove_resource(self, name):
        if name in self.resources:
            del self.resources[name]

        key = self.group + "/" + self.version + "/" + self.kind + "/" + name
        for parent in self._parents:
            if key in parent._collection:
                del parent._collection[key]


class PydanticResourceController(ClusterController):
    def __init__(
        self, resource_cls, itl: Itl, cluster: str, group: str, version: str, kind: str
    ):
        super().__init__(itl, cluster, group, version, kind)
        self.resource_cls = resource_cls

    async def create_resource(self, config):
        if "spec" not in config:
            raise ValueError("Config is missing required key: spec")
        return self.resource_cls(**config["spec"])


class SyncedResources:
    def __init__(self):
        self._collection = {}

    def register(self, itl: Itl, cluster, group, version, kind):
        def decorator(controller_cls):
            if issubclass(controller_cls, ClusterController):
                controller = controller_cls(itl, cluster, group, version, kind)
            elif issubclass(controller_cls, BaseModel):
                controller = PydanticResourceController(
                    controller_cls, itl, cluster, group, version, kind
                )

            self._register_controller(controller)
            itl.onconnect(controller.load_existing)
            return controller_cls

        return decorator

    def _register_controller(self, controller: ClusterController):
        controller._register_parent(self)
        for name, controller in controller.resources.items():
            key = (
                controller.group
                + "/"
                + controller.version
                + "/"
                + controller.kind
                + "/"
                + name
            )
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

    def __setitem__(self, name, value):
        self._collection[name] = value
