import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Tuple
import traceback

from ..itl import Itl
from ..clusters import BaseController, PendingOperation
from .resource_monitor import ResourceMonitor


class ResourceEvents:
    creation: asyncio.Event
    deletion: asyncio.Event
    creation_error = None
    deletion_error = None

    def __init__(self):
        self.creation = asyncio.Event()
        self.deletion = asyncio.Event()


async def _create_resource_if_not_exists(
    itl: Itl, cluster, group, version, kind, name, fiber, config, events: ResourceEvents
):
    resources = await itl.cluster_get(
        cluster,
        group=group,
        version=version,
        kind=kind,
        name=name,
        fiber=fiber,
    )
    if not resources:
        await itl.cluster_create(cluster, config)
    elif events:
        events.creation.set()


@dataclass(frozen=True)
class ResourceKey:
    cluster: str
    apiVersion: str
    kind: str
    name: str
    fiber: str


RelayDict = Dict[ResourceKey, Tuple[ResourceKey, ResourceEvents, Any]]


class ResourceController:
    _children: Dict[str, RelayDict] = defaultdict(
        lambda: defaultdict(lambda: (None, None, None))
    )

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
        self.itl: Itl = itl
        self.cluster = cluster
        self.group = group
        self.version = version
        self.kind = kind
        self.fiber = fiber
        self.name = name
        self._started = False
        self.key = None

    def start(self, key=None):
        if self._started:
            return
        self.key = key
        self._started = True

        self.itl.controller(
            self.cluster,
            self.group,
            self.version,
            self.kind,
            fiber=self.fiber,
            key=key,
            name=self.name,
        )(self._controller)

    def stop(self):
        self.itl.controller_detach(self.key)
        self._started = False

    async def _controller(self, pending: BaseController):
        async for op in pending:
            message = await op.message()
            if message != None:
                try:
                    await self.post_resource(op)
                    await op.accept()
                except Exception as e:
                    await op.reject()
                    print(
                        f"Failed to post resource {self.kind}/{message['metadata']['name']}: {e}"
                    )
                    traceback.print_exc()
                    print("(Still running)")
                continue

            new_config = await op.new_config()
            if new_config == None:
                # Delete the resource
                try:
                    await self.delete_resource(op)
                    await op.accept(delete=True)
                except Exception as e:
                    await op.reject()
                    print(f"Failed to delete resource {self.kind}/{pending.name}: {e}")
                    traceback.print_exc()
                    print("(Still running)")
                continue

            old_config = await op.old_config()

            try:
                if old_config == None:
                    new_config = await self.create_resource(op)
                else:
                    new_config = await self.update_resource(op)

                await op.accept(new_config)

            except Exception as e:
                await op.reject()
                print(f"Failed to load resource {self.kind}/{pending.name}: {e}")
                traceback.print_exc()
                print("(Still running)")
            continue

    async def create_resource(self, op: PendingOperation):
        pass

    async def update_resource(self, op: PendingOperation):
        return await self.create_resource(op)

    async def post_resource(self, op: PendingOperation):
        pass

    async def delete_resource(self, op: PendingOperation):
        pass

    async def create_child(
        self, parent_config, child_config, child_ref, controller=ResourceMonitor
    ):
        study_key = ResourceKey(
            self.cluster,
            parent_config["apiVersion"],
            parent_config["kind"],
            parent_config["metadata"]["name"],
            parent_config["metadata"].get("fiber", "resource"),
        )
        previous_child_key, previous_child_events, previous_controller = self._children[
            child_ref
        ][study_key]
        child_key = ResourceKey(
            self.cluster,
            child_config["apiVersion"],
            child_config["kind"],
            child_config["metadata"]["name"],
            child_config["metadata"].get("fiber", "resource"),
        )
        child_group, child_version = child_key.apiVersion.split("/")

        if previous_child_key != None and previous_child_key != child_key:
            # delete the previous optimizer
            (
                previous_child_group,
                previous_child_version,
            ) = previous_child_key.apiVersion.split("/")
            await self.itl.cluster_delete(
                self.cluster,
                previous_child_group,
                previous_child_version,
                previous_child_key.kind,
                previous_child_key.name,
                fiber=previous_child_key.fiber,
            )
            if previous_controller:
                await previous_child_events.deletion.wait()
                previous_controller.stop()
            del self._children[child_ref][study_key]
            if previous_child_events.deletion_error:
                raise previous_child_events.deletion_error

        if child_key != previous_child_key:
            new_child_events = ResourceEvents()
            new_child_controller = None

            if not isinstance(controller, (ResourceController, ResourceMonitor)):
                if hasattr(controller, "__call__"):
                    try:
                        controller = controller(
                            itl=self.itl,
                            cluster=self.cluster,
                            group=child_group,
                            version=child_version,
                            kind=child_key.kind,
                            fiber=child_key.fiber,
                            name=child_key.name,
                        )
                    except:
                        raise ValueError(
                            "controller function must support the same args as ResourceController and ResourceMonitor"
                        )

            if isinstance(controller, ResourceController):
                new_child_controller = CascadeChildController(
                    self.itl,
                    self.cluster,
                    child_group,
                    child_version,
                    child_key.kind,
                    child_key.fiber,
                    name=child_key.name,
                    controller=controller,
                    events=new_child_events,
                )

                # Normally there would be a gap between when the controller is started
                # and when it starts receiving messages, which would cause problems
                # when checking for existing resources. There's no such gap in this
                # scenario because to get to this point, the controller must have
                # already started receiving messages. So we can safely check for
                # existing resources immediately after starting the controller without
                # worrying about timing issues.
                new_child_controller.start(child_key)

            elif isinstance(controller, ResourceMonitor):
                new_child_controller = CascadeChildMonitor(
                    self.itl,
                    self.cluster,
                    child_group,
                    child_version,
                    child_key.kind,
                    child_key.fiber,
                    name=child_key.name,
                    controller=controller,
                    events=new_child_events,
                )

                new_child_controller.start(child_key)

            elif controller == None:
                new_child_events.creation.set()
            else:
                raise ValueError(
                    "controller must be a ResourceController, ResourceMonitor, or None"
                )

            self._children[child_ref][study_key] = (
                child_key,
                new_child_events,
                new_child_controller,
            )

            await _create_resource_if_not_exists(
                self.itl,
                self.cluster,
                child_group,
                child_version,
                child_key.kind,
                child_key.name,
                child_key.fiber,
                child_config,
                new_child_events,
            )

            await new_child_events.creation.wait()
            if new_child_events.creation_error:
                raise new_child_events.creation_error

    async def create_children(
        self, parent_config, child_configs: dict, sync=False, controller=ResourceMonitor
    ):
        if sync:
            tasks = []
            for child_ref, child_config in child_configs.items():
                tasks.append(
                    self.create_child(
                        parent_config, child_config, child_ref, controller=controller
                    )
                )
            await asyncio.gather(*tasks)
        else:
            for child_ref, child_config in child_configs.items():
                asyncio.create_task(
                    self.create_child(
                        parent_config, child_config, child_ref, controller=controller
                    )
                )

    async def delete_child(self, parent_config, child_ref):
        study_key = ResourceKey(
            self.cluster,
            parent_config["apiVersion"],
            parent_config["kind"],
            parent_config["metadata"]["name"],
            parent_config["metadata"].get("fiber", "resource"),
        )
        (
            previous_child_key,
            previous_child_events,
            previous_child_controller,
        ) = self._children[child_ref][study_key]
        child_group, child_version = previous_child_key.apiVersion.split("/")

        if previous_child_key != None:
            # delete the previous optimizer
            await self.itl.cluster_delete(
                self.cluster,
                child_group,
                child_version,
                previous_child_key.kind,
                previous_child_key.name,
                fiber=previous_child_key.fiber,
            )
            if previous_child_controller:
                await previous_child_events.deletion.wait()
                previous_child_controller.stop()
            del self._children[child_ref][study_key]
            if previous_child_events.deletion_error:
                raise previous_child_events.deletion_error

    async def delete_children(self, parent_config, *child_refs, sync=True):
        if sync:
            tasks = []
            for child_ref in child_refs:
                tasks.append(self.delete_child(parent_config, child_ref))
            await asyncio.gather(*tasks)
        else:
            for child_ref in child_refs:
                asyncio.create_task(self.delete_child(parent_config, child_ref))


class CascadeChildController(ResourceController):
    def __init__(
        self,
        itl,
        cluster,
        group,
        version,
        kind,
        fiber,
        name,
        controller,
        events,
    ):
        super().__init__(itl, cluster, group, version, kind, fiber, name)
        self.controller: ResourceController = controller
        self.events: ResourceEvents = events

    async def create_resource(self, op: PendingOperation):
        try:
            result = await self.controller.create_resource(op)
            self.events.creation.set()
            return result
        except Exception as e:
            self.events.creation_error = e
            self.events.creation.set()
            raise e

    async def update_resource(self, op: PendingOperation):
        try:
            result = await self.controller.update_resource(op)
            self.events.creation.set()
            return result
        except Exception as e:
            self.events.creation_error = e
            self.events.creation.set()
            raise e

    async def delete_resource(self, op: PendingOperation):
        try:
            result = await self.controller.delete_resource(op)
            self.events.deletion.set()
            return result
        except Exception as e:
            self.events.deletion_error = e
            self.events.deletion.set()
            raise e

    async def post_resource(self, op: PendingOperation):
        try:
            return await self.controller.post_resource(op)
        except Exception as e:
            raise e


class CascadeChildMonitor(ResourceMonitor):
    def __init__(
        self,
        itl,
        cluster,
        group,
        version,
        kind,
        fiber,
        name,
        controller,
        events,
    ):
        super().__init__(itl, cluster, group, version, kind, fiber, name)
        self.controller: ResourceMonitor = controller
        self.events: ResourceEvents = events

    async def onput(self, config):
        try:
            result = await self.controller.onput(config)
            self.events.creation.set()
            return result
        except Exception as e:
            self.events.creation_error = e
            self.events.creation.set()
            raise e

    async def ondelete(self, resource):
        try:
            result = await self.controller.ondelete(resource)
            self.events.deletion.set()
            return result
        except Exception as e:
            self.events.deletion_error = e
            self.events.deletion.set()
            raise e
