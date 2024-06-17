from typing import Any
import traceback

from ..itl import Itl


class ResourceMonitor:
    def __init__(
        self,
        itl: Itl,
        cluster: str,
        group: str,
        version: str,
        kind: str,
        fiber: str = "resource",
        name: str = None,
    ):
        self.itl = itl
        self.cluster = cluster
        self.group = group
        self.version = version
        self.kind = kind
        self.fiber = fiber
        self.name = name
        self.key = None
        self._cluster_id = itl.get_resource("Cluster", cluster).id
        self._resources_by_name: dict[int, Any] = {}
        self._next_id = 0

    def start(self, key=None):
        self.key = key
        self.itl.onupdate(
            self.cluster,
            group=self.group,
            version=self.version,
            fiber=self.fiber,
            key=key,
            onconnect=self._load_existing,
            name=self.name,
        )(self._handle_event)

    def stop(self):
        self.itl.stream_detach(self.key)

    async def _handle_event(self, event, cluster, name, **data):
        if event == "put":
            config = await self.itl.cluster_get(
                self.cluster,
                self.group,
                self.version,
                self.kind,
                name,
                self.fiber,
                cluster,
            )

            try:
                self._resources_by_name[name] = await self.onput(config)
            except:
                traceback.print_exc()
                print("(Still running)")

        elif event == "delete":
            if name not in self._resources_by_name:
                return
            try:
                await self.ondelete(self._resources_by_name[name])
            except:
                traceback.print_exc()
                print("(Still running)")
            del self._resources_by_name[name]

    async def onput(self, config):
        return config

    async def ondelete(self, resource):
        pass

    def __getitem__(self, name):
        return self._resources_by_name[name]

    def get(self, name, default=None):
        return self._resources_by_name.get(name, default)

    async def _load_existing(self):
        # TODO: check timestamps to avoid reloading resources
        resources = await self.itl.cluster_get_all(
            self.cluster,
            group=self.group,
            version=self.version,
            kind=self.kind,
            fiber=self.fiber,
        )
        if resources:
            for data in resources:
                config = data["config"]
                name = config["metadata"]["name"]
                try:
                    self._resources_by_name[name] = await self.onput(config)
                except:
                    traceback.print_exc()
                    print("(Still running)")
