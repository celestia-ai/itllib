from typing import Dict
import asyncio
import functools
import traceback

from itllib.itl import Itl

from .resource_controller import ResourceKey
from .instance_controller import InstanceController
from ..clusters import BaseController, PendingOperation


class RelayChildController(InstanceController):
    def __init__(self, callback, parent_config, child_ref, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.callback = callback
        self.parent_config = parent_config
        self.child_ref = child_ref

    async def post_resource(self, op: PendingOperation):
        return await self.callback(self.parent_config, self.child_ref, op)


class RelayOperator(InstanceController):
    async def unlock_resources(self):
        unlock_tasks = [super().unlock_resources()]

        relay_resources = await self.itl.cluster_get_all(
            self.cluster,
            group=self.group,
            version=self.version,
            kind=self.kind,
            fiber=self.fiber,
        )

        for relay_data in relay_resources:
            relay_config = relay_data["config"]
            children = await self.get_relays(relay_config)
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

    async def get_relays(self, config) -> Dict[str, ResourceKey]:
        raise NotImplementedError()

    async def load_resource(self, config):
        await self.start_relays(config)

    async def create_resource(self, op: PendingOperation):
        await self.start_relays(await op.new_config())

    async def delete_resource(self, op: PendingOperation):
        await self.stop_relays(await op.old_config())

    async def start_relays(self, parent_config):
        print("starting relays for", parent_config)
        # create the proxy resources, wait for them to be ready
        create_child_tasks = []
        for key, child_key in (await self.get_relays(parent_config)).items():
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
        children = await self.get_relays(config)
        await self.delete_children(config, *children.keys())
