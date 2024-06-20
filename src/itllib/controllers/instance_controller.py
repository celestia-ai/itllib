import asyncio

from .resource_controller import ResourceController


class InstanceController(ResourceController):
    async def unlock_resources(self):
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

        await asyncio.gather(*unlock_tasks)

    def start(self, key=None):
        _start_fn = super().start

        async def _start():
            await self.unlock_resources()
            await self._load_existing()
            # self.start_controller()
            _start_fn(key)

        self.itl.onconnect(_start)

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
            await self.load_resource(config)

    async def load_resource(self, config):
        pass
