import asyncio
from collections import defaultdict
from typing import Any
from pydantic import BaseModel

from itllib.resources import ClusterResource


from .itl import Itl
from .clusters import BaseController, merge as apply_patch


class ResourceController:
    def __init__(
        self,
        itl: Itl,
        from_cluster: str,
        group: str,
        version: str,
        kind: str,
        fiber: str = "resource",
    ):
        self.itl: Itl = itl
        self.from_cluster = from_cluster
        self.group = group
        self.version = version
        self.kind = kind
        self.fiber = fiber
        self._xx_deleteme_parents: list[SyncedResources] = []
        self._xx_deleteme_resources: dict[str, Any] = {}
        self._started = False

    def start(self):
        if self._started:
            return
        self._started = True

        self.itl.controller(
            self.from_cluster, self.group, self.version, self.kind, fiber=self.fiber
        )(self.controller)

    async def controller(self, pending: BaseController):
        async for op in pending:
            message = await op.message()
            if message != None:
                try:
                    await self.post_resource(pending.cluster, message)
                    await op.accept()
                except Exception as e:
                    await op.reject()
                    print(
                        f"Failed to post resource {self.kind}/{message['metadata']['name']}: {e}"
                    )
                continue

            new_config = await op.new_config()
            if new_config == None:
                # Delete the resource
                try:
                    await self.delete_resource(
                        pending.cluster,
                        pending.group,
                        pending.version,
                        pending.kind,
                        pending.name,
                        pending.fiber,
                    )
                    await op.accept(delete=True)
                except Exception as e:
                    await op.reject()
                    print(f"Failed to delete resource {self.kind}/{pending.name}: {e}")
                continue

            old_config = await op.old_config()

            try:
                if old_config == None:
                    await self.create_resource(pending.cluster, new_config)
                else:
                    await self.update_resource(
                        pending.cluster, new_config
                    )

                await op.accept()

            except Exception as e:
                await op.reject()
                print(f"Failed to load resource {self.kind}/{pending.name}: {e}")
            continue

    async def create_resource(self, cluster, new_config):
        raise ValueError("create_resource not implemented")

    async def update_resource(self, cluster, new_config):
        return await self.create_resource(cluster, new_config)

    async def post_resource(self, cluster, message):
        pass

    async def delete_resource(
        self, cluster, group, version, kind, name, fiber
    ):
        pass

    # async def put(self, config):
    #     if "apiVersion" not in config:
    #         raise ValueError("Config is missing required key: apiVersion")
    #     if config["apiVersion"] != f"{self.group}/{self.version}":
    #         raise ValueError(
    #             f'Config apiVersion does not match resource: {config["apiVersion"]} != {self.group}/{self.version}'
    #         )

    #     if "kind" not in config:
    #         raise ValueError("Config is missing required key: kind")
    #     if config["kind"] != self.kind:
    #         raise ValueError(
    #             f'Config kind does not match resource: {config["kind"]} != {self.kind}'
    #         )

    #     if "metadata" not in config:
    #         raise ValueError("Config is missing required key: metadata")
    #     metadata = config["metadata"]
    #     if not isinstance(metadata, dict):
    #         raise ValueError("Config metadata must be a dictionary")

    #     if "name" not in config["metadata"]:
    #         raise ValueError("Config is missing required key: metadata.name")
    #     name = config["metadata"]["name"]
    #     if not isinstance(name, str):
    #         raise ValueError("Config metadata.name must be a string")

    #     cluster = metadata.get("remote", self.from_cluster)

    #     if not name in self._xx_deleteme_resources:
    #         resource = await self.create_resource(cluster, config)
    #         self._add_resource(cluster, name, resource)
    #     else:
    #         resource = self._xx_deleteme_resources[name]
    #         resource = await self.update_resource(cluster, config, resource)
    #         self._add_resource(cluster, name, resource)

    #     await self.itl.cluster_apply(self.from_cluster, config)

    # async def delete(self, name):
    #     if name in self._xx_deleteme_resources:
    #         resource = self._xx_deleteme_resources[name]
    #         await self.delete_resource(resource)
    #         self._remove_resource(name)
    #         await self.itl.cluster_delete(
    #             self.group, self.version, self.kind, name, cluster=self.from_cluster
    #         )

    # async def patch(self, name, patch):
    #     if "apiVersion" not in patch:
    #         patch["apiVersion"] = f"{self.group}/{self.version}"
    #     else:
    #         if patch["apiVersion"] != f"{self.group}/{self.version}":
    #             raise ValueError(
    #                 f'Patch apiVersion does not match resource: {patch["apiVersion"]} != {self.group}/{self.version}'
    #             )
    #     if "kind" not in patch:
    #         patch["kind"] = self.kind
    #     else:
    #         if patch["kind"] != self.kind:
    #             raise ValueError(
    #                 f'Patch kind does not match resource: {patch["kind"]} != {self.kind}'
    #             )
    #     if "metadata" not in patch:
    #         patch["metadata"] = {"name": name}
    #     else:
    #         metadata = patch["metadata"]
    #         if not isinstance(metadata, dict):
    #             raise ValueError("Patch metadata must be a dictionary")
    #         if "name" not in metadata:
    #             metadata["name"] = name
    #         else:
    #             if metadata["name"] != name:
    #                 raise ValueError(
    #                     f'Patch metadata.name does not match resource name: {metadata["name"]} != {name}'
    #                 )

    #     cluster = metadata.get("remote", self.from_cluster)

    #     old_config = await self.itl.cluster_read(
    #         self.from_cluster, self.group, self.version, self.kind, name
    #     )
    #     if not old_config:
    #         raise ValueError(f"No resource found for {self.kind}/{name}")
    #     new_config = apply_patch(old_config, patch)

    #     if name not in self._xx_deleteme_resources:
    #         resource = await self.create_resource(cluster, new_config)
    #         self._add_resource(cluster, name, resource)
    #     else:
    #         resource = self._xx_deleteme_resources[name]
    #         resource = await self.update_resource(cluster, new_config, resource)
    #         self._add_resource(cluster, name, resource)

    #     await self.itl.cluster_patch(self.from_cluster, patch)



class SyncedResources:
    def __init__(self, itl: Itl, cluster: str, group: str, version: str, fiber: str = "resource"):
        self.itl = itl
        self.cluster = cluster
        self.group = group
        self.version = version
        self.fiber = fiber
        self._cluster_id = itl.get_resource('Cluster', cluster).id
        self._resources_by_id: dict[int, BaseModel] = {}
        self._resource_ids_by_path: dict[str, dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))
        self._resource_ids_by_class: dict[type, dict[str, int]] = defaultdict(dict)
        self._classes_by_kind: dict[str, set[type]] = defaultdict(set)
        self._next_id = 0
    
    def start(self):
        self.itl.onupdate(self.cluster, group=self.group, version=self.version, fiber=self.fiber)(self.handle_event)
    
    def _query_ids(self, cluster=None, kind=None, name=None, cls=None):
        resource_ids = None
        if cluster is not None:
            if resource_ids == None:
                resource_ids = self._resource_ids_by_path['cluster'][cluster]
            else:
                resource_ids &= self._resource_ids_by_path['cluster'][cluster]
        if kind is not None:
            if resource_ids == None:
                resource_ids = self._resource_ids_by_path['kind'][kind]
            else:
                resource_ids &= self._resource_ids_by_path['kind'][kind]
        if name is not None:
            if resource_ids == None:
                resource_ids = self._resource_ids_by_path['name'][name]
            else:
                resource_ids &= self._resource_ids_by_path['name'][name]
        if cls is not None:
            if resource_ids == None:
                resource_ids = set(self._resource_ids_by_class[cls].values())
            else:
                resource_ids &= set(self._resource_ids_by_class[cls].values())
        
        if resource_ids == None:
            return list(self._resources_by_id.keys())
        
        return list(resource_ids)
    
    def _handle_put(self, resource_id, cluster, kind, name, cls, config):
        new_resource = cls(**config)
        self._resources_by_id[resource_id] = new_resource
        self._resource_ids_by_class[cls][name] = resource_id
        self._resource_ids_by_path['cluster'][cluster].add(resource_id)
        self._resource_ids_by_path['kind'][kind].add(resource_id)
        self._resource_ids_by_path['name'][name].add(resource_id)

    def _handle_delete(self, resource_id, cluster, kind, name, cls):
        del self._resources_by_id[resource_id]
        del self._resource_ids_by_class[cls][name]
        self._resource_ids_by_path['cluster'][cluster].remove(resource_id)
        self._resource_ids_by_path['kind'][kind].remove(resource_id)
        self._resource_ids_by_path['name'][name].remove(resource_id)
    
    def _get_resource_id(self, cluster, kind, name, cls, required=False):
        resource_id = None
        candidates = self._query_ids(cluster=cluster, kind=kind, name=name, cls=cls)
        if candidates:
            resource_id = candidates[0]
        
        if required and resource_id == None:
            resource_id = self._next_id
            self._next_id += 1
        
        return resource_id


    async def handle_event(self, event, cluster, kind, name, **data):
        print('got event for', event, cluster, kind, name, data)
        print('have classes:', self._classes_by_kind[kind])
        for cls in self._classes_by_kind[kind]:            
            if event == 'put':
                resource_id = self._get_resource_id(cluster, kind, name, cls, required=True)
                config = await self.itl.cluster_read(self.cluster, self.group, self.version, kind, name, self.fiber, cluster)
                print('got config:', config)
                self._handle_put(resource_id, cluster, kind, name, cls, config)

            elif event == 'delete':
                resource_id = self._get_resource_id(cluster, kind, name, cls, required=False)
                if resource_id == None:
                    continue
                self._handle_delete(resource_id, cluster, kind, name, cls)


    def register(self, kind=None):
        def decorator(controller_cls: type):
            kind_name = kind or controller_cls.__name__
            self._classes_by_kind[kind_name].add(controller_cls)

            async def preload():
                await self.load_existing(kind_name, controller_cls)
            self.itl.onconnect(preload)
            
            return controller_cls

        return decorator

    def query(self, cluster=None, kind=None, name=None, cls=None):
        for resource_id in self._query_ids(cluster=cluster, kind=kind, name=name, cls=cls):
            yield self._resources_by_id[resource_id]
    
    async def load_existing(self, kind: str, cls: type):
        resources = await self.itl.cluster_read_all(
            self.cluster, group=self.group, version=self.version, kind=kind, fiber=self.fiber
        )
        if resources:
            for data in resources:
                cluster = data["cluster"]
                config = data['config']
                name = config["metadata"]["name"]
                resource_id = self._get_resource_id(cluster, kind, name, cls, required=True)
                self._handle_put(resource_id, cluster, kind, name, cls, config)
