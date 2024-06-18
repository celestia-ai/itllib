from typing import Dict
import asyncio
import functools
import traceback

from itllib.itl import Itl

from .resource_controller import ResourceController, ResourceKey
from ..clusters import BaseController, PendingOperation


class RelayChildController(ResourceController):
    def __init__(self, callback, parent_config, child_ref, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.callback = callback
        self.parent_config = parent_config
        self.child_ref = child_ref

    async def post_resource(self, op: PendingOperation):
        return await self.callback(self.parent_config, self.child_ref, op)


class RelayOperator(ResourceController):
    # _relays['optimizers']: Dict[ResourceKey, Tuple[ResourceKey, ProxyEvents]] = defaultdict(lambda: (None, None))
    # _relays['experimenters']: Dict[ResourceKey, Tuple[ResourceKey, ProxyEvents]] = defaultdict(lambda: (None, None))

    def __init__(self, itl: Itl, cluster, group, version, kind, fiber="resource"):
        self.itl = itl
        self.cluster = cluster
        self.group = group
        self.version = version
        self.kind = kind
        self.fiber = fiber

    async def unlock_all(self):
        relay_locks = await self.itl.cluster_read_queue(
            self.cluster,
            group=self.group,
            version=self.version,
            kind=self.kind,
            fiber=self.fiber,
        )

        unlock_tasks = [
            self.itl.cluster_unlock(
                self.cluster,
                relay_key["group"],
                relay_key["version"],
                relay_key["kind"],
                relay_key["name"],
                relay_key["fiber"],
            )
            for relay_key in relay_locks
        ]

        relay_resources = await self.itl.cluster_get_all(
            self.cluster,
            group=self.group,
            version=self.version,
            kind=self.kind,
            fiber=self.fiber,
        )

        for relay_data in relay_resources:
            relay_config = relay_data["config"]
            children = await self.get_children(relay_config)
            for child_key in children.values():
                child_group, child_version = child_key.apiVersion.split("/")
                unlock_tasks.append(
                    self.itl.cluster_unlock(
                        self.cluster,
                        child_group,
                        child_version,
                        child_key.kind,
                        child_key.name,
                        child_key.fiber,
                    )
                )

        await asyncio.gather(*unlock_tasks)

    async def get_children(self, config) -> Dict[str, ResourceKey]:
        raise NotImplementedError()

    async def _start(self):
        await self.unlock_all()
        await self._load_existing()
        self.start_controller()

    def start(self):
        self.itl.onconnect(self._start)

    def start_controller(self, key=None):
        # Start listening for config ops

        @self.itl.controller(
            self.cluster,
            group=self.group,
            version=self.version,
            kind=self.kind,
            fiber=self.fiber,
            key=key,
            # onconnect=self._load_existing,
        )
        async def handle_updates(pending: BaseController):
            async for op in pending:
                message = await op.message()
                old_config = await op.old_config()
                new_config = await op.new_config()

                if message != None:
                    pass
                elif new_config != None:
                    try:
                        await self.start_relays(new_config)
                    except Exception as e:
                        print("Error starting proxies for:", new_config)
                        traceback.print_exc()
                        await op.reject()
                        print("(Still running)")
                        continue

                elif new_config == None:
                    try:
                        await self.stop_relays(old_config)
                    except Exception as e:
                        print("Error stopping proxies for:", old_config)
                        traceback.print_exc()
                        await op.reject()
                        print("(Still running)")
                        continue

                await op.accept()

    async def _load_existing(self):
        resources = await self.itl.cluster_get_all(
            self.cluster,
            group=self.group,
            version=self.version,
            kind=self.kind,
            fiber=self.fiber,
        )
        if not resources:
            return

        for data in resources:
            config = data["config"]
            await self.start_relays(config)

    async def start_relays(self, parent_config):
        # create the proxy resources, wait for them to be ready
        children = {}
        create_child_tasks = []
        for key, child_key in (await self.get_children(parent_config)).items():
            child_config = {
                "apiVersion": child_key.apiVersion,
                "kind": child_key.kind,
                "metadata": {
                    "name": child_key.name,
                    "fiber": child_key.fiber,
                },
            }

            create_relay_child_controller = functools.partial(
                RelayChildController,
                callback=self.relay_message,
                parent_config=parent_config,
                child_ref=key,
            )
            create_child_tasks.append(
                self.create_child(
                    parent_config, child_config, key, create_relay_child_controller
                )
            )

        await asyncio.gather(*create_child_tasks)

    async def relay_message(self, parent_config, child_ref, op: PendingOperation):
        raise NotImplementedError()

    async def stop_relays(self, config):
        # delete the proxy resources, wait for them to be deleted
        children = await self.get_children(config)
        await self.delete_children(config, *children.keys())
