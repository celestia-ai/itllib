import asyncio
from collections import defaultdict
import inspect
import threading
import typing
from glob import glob
import traceback
from urllib.parse import urlparse

import websockets
import json
import aiohttp
import yaml
import requests

from itllib.resources import (
    ResourcePile,
    ClientResource,
    ApiKeyResource,
    GroupResource,
    LoopResource,
    ResourceReference,
    ResourceResolver,
    StreamResource,
    ClusterResource,
)

from .piles import BucketOperations, PileOperations
from .clusters import ClusterOperations
from .loops import (
    ConnectionInfo,
    LoopOperations,
    StreamConnectionInfo,
    StreamOperations,
)


class Namespace:
    pass


def _get_expected_arguments(func):
    signature = inspect.signature(func)
    return set(signature.parameters.keys())


def _get_argument_type_hints(func):
    type_hints = typing.get_type_hints(func)
    signature = inspect.signature(func)
    argument_type_hints = {}

    for name, param in signature.parameters.items():
        if name in type_hints:
            argument_type_hints[name] = type_hints[name]

    return argument_type_hints


class Itl:
    def __init__(self, *configs, client=None) -> None:
        # User-specified handlers
        self._data_handlers = {}
        self._controllers = {}

        # Async stuff
        self._connection_thread = None
        self._connection_looper = None
        self._connection_tasks = asyncio.Queue()
        self._callback_thread = None
        self._callback_looper = None
        self._callback_tasks = asyncio.Queue()

        # Old stuff, to be removed
        self._stop = False

        # Resources
        self._secrets = {}
        self._streams: dict[str, StreamOperations] = {}
        self._buckets = {}
        self._piles = {}
        self._clusters: dict[str, ClusterOperations] = {}
        self._loops: dict[str, LoopOperations] = {}

        # Stream interactions
        self._downstreams = {}
        self._upstreams = {}
        self._start_persistent_tasks = {}
        self._downstream_queues = defaultdict(asyncio.Queue)
        self._started_streams = set()
        self._start_ephemeral_tasks = []
        self._post_connection_tasks = []

        self._resource_pile = ResourcePile()
        self._resolver = ResourceResolver(self._resource_pile)
        self._keys = defaultdict(lambda: None)

        self._apply_config(*configs, client=client)

    def _apply_config(self, *files, client=None):
        self._resource_pile.add(*files)
        self._resolver = ResourceResolver(self._resource_pile)

        # Get apikeys for the client
        if client != None:
            client_ref = ResourceReference("Client", client)
            client_resource = self._resolver.get_resource_by_reference(client_ref)
            assert isinstance(client_resource, ClientResource)
            client_id = client_resource.id(self._resolver)

            for apikey in self._resolver.apikeys_for_client(client_id):
                remote = apikey.get_remote(self._resolver)
                self._keys[remote] = apikey.key(self._resolver)

        for resource in self._resource_pile.compiled_resources.values():
            if isinstance(resource, LoopResource):
                remote = resource.get_remote(self._resolver)
                connection_info = resource.connection_info(self._resolver)
                self._loops[resource.name] = LoopOperations(
                    connection_info, self._keys[remote]
                )
            elif isinstance(resource, StreamResource):
                remote = resource.get_remote(self._resolver)
                connection_info = resource.connection_info(self._resolver)
                self._streams[resource.name] = StreamOperations(
                    connection_info, self._keys[remote]
                )
            elif isinstance(resource, ClusterResource):
                remote = resource.get_remote(self._resolver)
                connection_info = resource.connection_info(self._resolver)
                self._clusters[resource.name] = ClusterOperations(
                    connection_info, self._keys[remote]
                )
                self._streams["cluster/" + resource.name] = StreamOperations(
                    StreamConnectionInfo(None, connection_info.connect_info),
                    self._keys[remote],
                )

    def attach_pile_prefix(self, pile, name):
        return self._piles[pile].prefix + name

    def object_download(self, pile, key=None, notification=None, attach_prefix=False):
        if key == None and notification == None:
            raise ValueError("Exactly one of key or event must be provided")
        if key != None and notification != None:
            raise ValueError("Only one of key or event can be provided")

        if notification != None:
            key = notification["key"]

        pile_ops = self._piles[pile]

        if attach_prefix:
            key = f"{pile_ops.prefix or ''}{key}"

        return pile_ops.get(key)

    def object_upload(
        self, pile, key, file_descriptor, metadata={}, attach_prefix=False
    ):
        pile_ops = self._piles[pile]

        if attach_prefix:
            key = f"{pile_ops.prefix or ''}{key}"

        return pile_ops.put(key, file_descriptor, metadata)

    def object_delete(self, pile, key=None, attach_prefix=False):
        pile_ops = self._piles[pile]

        if attach_prefix:
            key = f"{pile_ops.prefix or ''}{key}"

        return pile_ops.delete(key)

    async def cluster_create(self, cluster, data):
        return await self._clusters[cluster].create_resource(data)

    async def cluster_read_all(
        self, cluster, group=None, version=None, kind=None, name=None, utctime=None
    ):
        return await self._clusters[cluster].read_all_resources(
            group, version, kind, name, utctime
        )

    async def cluster_read(self, cluster, group, version, kind, name):
        return await self._clusters[cluster].read_resource(group, version, kind, name)

    async def cluster_patch(self, cluster, data):
        return await self._clusters[cluster].patch_resource(data)

    async def cluster_update(self, cluster, data):
        return await self._clusters[cluster].update_resource(data)

    async def cluster_apply(self, cluster, data):
        return await self._clusters[cluster].apply_resource(data)

    async def cluster_delete(self, cluster, group, version, kind, name):
        return await self._clusters[cluster].delete_resource(group, version, kind, name)

    def cluster_controller(self, cluster, group, version, kind, name, validate=True):
        cluster_obj = self._clusters[cluster]
        return cluster_obj.control_resource(
            group, version, kind, name, validate=validate
        )
        # yield controller

    def _get_url(self, identifier) -> str:
        if identifier in self._streams:
            return self._streams[identifier].connect_url
        else:
            return identifier

    def _ensure_stream_connection(self, streams):
        """
        Update the upstream tasks based on the provided streams. If an upstream task for a
        given identifier already exists, it is skipped. If the looper isn't initialized,
        a task to attach the downstream is created. If the looper is initialized, a new downstream task
        is scheduled to run asynchronously.

        Args:
        - streams (List[str]): List of stream identifiers to be processed.

        Returns:
        None
        """
        for identifier in streams:
            # Skip creating a task if it already exists
            if identifier in self._start_persistent_tasks:
                continue

            # Check if the Itl is already running
            if not self._connection_looper:
                task = self._attach_stream, (identifier,)
                self._start_persistent_tasks[identifier] = task
            else:
                self._connection_looper.call_soon_threadsafe(
                    self._connection_tasks.put_nowait,
                    lambda: self._schedule_stream_task_unsafe(identifier),
                )

    def _schedule_stream_task_unsafe(self, identifier):
        """
        Schedules an upstream task for the given identifier if it doesn't already exist.

        Args:
        - identifier (str): Identifier for the stream.

        Returns:
        None
        """
        if identifier in self._start_persistent_tasks:
            return

        task = self._attach_stream, (identifier,)
        self._start_persistent_tasks[identifier] = task
        asyncio.create_task(self._attach_stream(identifier))

    def ondata(self, stream=None, loop=None, streamId=None):
        if stream == None:
            loop_info = self._loops[loop]
            stream = f'stream/{loop}/{streamId}'
            if stream not in self._streams:
                self._streams[stream] = StreamOperations(
                    loop_info.connect_info.stream_connection_info(streamId),
                    loop_info.apikey,
                )
        if stream not in self._upstreams:
            self._ensure_stream_connection([stream])

        def decorator(func):
            self._data_handlers.setdefault(stream, []).append(func)
            return func

        return decorator

    def controller(
        self,
        cluster,
        group=None,
        version=None,
        kind=None,
        name=None,
        validate=True,
    ):
        cluster_obj = self._clusters[cluster]
        stream = "cluster/" + cluster

        def decorator(func):
            async def controller_wrapper(*args, **event):
                operations = self.cluster_controller(
                    cluster,
                    event["group"],
                    event["version"],
                    event["kind"],
                    event["name"],
                    validate=validate,
                )
                async with operations:
                    try:
                        await func(operations)
                    except Exception as e:
                        print(
                            f"Error in controller {func.__name__}: {traceback.format_exc()}"
                        )

            @self.ondata(stream)
            async def event_handler(*args, **event):
                if event["event"] != "queue":
                    return
                if group and event["group"] != group:
                    return
                if version and event["version"] != version:
                    return
                if kind and event["kind"] != kind:
                    return

                asyncio.create_task(controller_wrapper(*args, **event))

            self._controllers.setdefault(cluster, []).append(func)

            async def check_queue():
                for queued_op in await cluster_obj.read_queue(
                    group, version, kind, name
                ):
                    asyncio.create_task(controller_wrapper(**queued_op))

            self.onconnect(check_queue)
            return func

        return decorator

    def onconnect(self, func):
        if self._callback_looper:
            self._callback_looper.call_soon_threadsafe(
                self._callback_tasks.put_nowait, (func, ())
            )
        else:
            self._start_ephemeral_tasks.append((func, ()))

        return func

    async def _process_upstream_messages(self):
        def exec_callback(handler, args, kwargs):
            try:
                if inspect.iscoroutinefunction(handler):
                    asyncio.create_task(handler(*args, **kwargs))
                else:
                    handler(*args, **kwargs)
            except Exception as e:
                print(f"Error in handler {handler.__name__}: {traceback.format_exc()}")

        def process_message(identifier, serialized_data):
            message = json.loads(serialized_data)
            # TODO: Run all handlers in parallel
            for handler in self._data_handlers[identifier]:
                if isinstance(message, dict):
                    args = []
                    kwargs = message
                else:
                    args = [message]
                    kwargs = {}

                exec_callback(handler, args, kwargs)

        while True:
            task = await self._callback_tasks.get()

            if self._stop:
                break

            if task == None:
                continue

            identifier, serialized_data = task

            try:
                if isinstance(identifier, str):
                    process_message(identifier, serialized_data)
                else:
                    exec_callback(identifier, serialized_data, {})
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"Error in message processing: {traceback.format_exc()}")

    async def _post_stream_message(self, url, message):
        # call HTTP POST on key, passing message as data
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json.dumps(message)) as response:
                response.raise_for_status()

    def stream_send(self, key=None, message=None, loop=None, streamId=None):
        if key == None:
            loop_info = self._loops[loop]
            key = f'stream/{loop}/{streamId}'
            if key not in self._streams:
                self._streams[key] = StreamOperations(
                    loop_info.connect_info.stream_connection_info(streamId),
                    loop_info.apikey,
                )

        if key not in self._streams and (
            key.startswith("http://") or key.startswith("https://")
        ):
            asyncio.run_coroutine_threadsafe(
                self._post_stream_message(key, message), self._connection_looper
            )
            return

        self._ensure_stream_connection([key])

        if self._connection_looper:
            self._connection_looper.call_soon_threadsafe(
                self._downstream_queues[key].put_nowait, message
            )
        else:
            task = self._downstream_queues[key].put, (message,)
            self._connection_looper.call_soon_threadsafe(
                self._downstream_queues[key].put_nowait, task
            )

    def stream_send_sync(self, key, message):
        if key.startswith("http://") or key.startswith("https://"):
            # call HTTP POST on key, passing message as data
            requests.post(url=key, json=json.dumps(message))
            return

        self._ensure_stream_connection([key])

        if self._connection_looper:
            self._downstream_queues[key].put_nowait(message)
        else:
            task = self._downstream_queues[key].put, (message,)
            self._downstream_queues[key].put_nowait(task)

    def _requeue(self, identifier, message):
        if message == None:
            return

        old_queue = self._downstream_queues[identifier]
        new_queue = asyncio.Queue()
        if message != None:
            new_queue.put_nowait(message)
        while not old_queue.empty():
            new_queue.put_nowait(old_queue.get_nowait())

        self._downstream_queues[identifier] = new_queue

    async def _attach_stream(self, identifier):
        if identifier in self._started_streams:
            return

        self._started_streams.add(identifier)
        state = Namespace()
        state.message = None

        if identifier not in self._streams:
            raise NotImplementedError("Streams must be defined in the config")

        apikey = self._streams[identifier].apikey

        async def send_message():
            state.message = (
                state.message or await self._downstream_queues[identifier].get()
            )

            if self._stop:
                self._requeue(identifier, state.message)
                return False

            serialized_data = json.dumps(state.message)

            try:
                await self._streams[identifier].send(serialized_data)
            except websockets.exceptions.ConnectionClosedError:
                return False
            except websockets.exceptions.ConnectionClosedOK:
                return False

            state.message = None
            return True

        async def recv_message():
            try:
                serialized_data = await self._streams[identifier].recv()
            except websockets.exceptions.ConnectionClosedError:
                return False
            except websockets.exceptions.ConnectionClosedOK:
                return False

            # If there are no data handlers for this identifier, skip processing
            if identifier not in self._data_handlers:
                return True

            self._callback_looper.call_soon_threadsafe(
                self._callback_tasks.put_nowait, (identifier, serialized_data)
            )

            return True

        backoff_time = 0
        tasks = None

        while not self._stop:
            try:
                if not tasks:
                    tasks = [
                        asyncio.create_task(asyncio.sleep(0)),
                        asyncio.create_task(asyncio.sleep(0)),
                    ]

                ws_url = self._get_url(identifier)
                if apikey:
                     ws_url = ws_url + "/apikey/" + apikey

                async with websockets.connect(ws_url) as websocket:
                    backoff_time = 0
                    self._streams[identifier].socket = websocket

                    while True:
                        done, pending = await asyncio.wait(
                            tasks, return_when=asyncio.FIRST_COMPLETED
                        )
                        if self._stop:
                            return

                        connection_closed = False

                        for completed in done:
                            if completed.result() == False:
                                connection_closed = True
                                tasks = None
                            elif completed == tasks[0]:
                                tasks[0] = asyncio.create_task(send_message())
                            elif completed == tasks[1]:
                                tasks[1] = asyncio.create_task(recv_message())

                        if connection_closed:
                            break

            except websockets.exceptions.ConnectionClosedOK:
                pass
            except websockets.exceptions.ConnectionClosedError:
                pass

            if self._stop:
                return

            # Backoff before reconnecting
            backoff_time = await self._exponential_backoff(backoff_time)

    async def _exponential_backoff(self, current_backoff_time):
        """Sleeps the process for an exponential backoff time."""
        await asyncio.sleep(2**current_backoff_time)
        # Return the next backoff time, capped at 2**7 seconds
        return min(current_backoff_time + 1, 7)

    async def _connect(self):
        connection_tasks = []
        for fn, args in self._start_persistent_tasks.values():
            asyncio.create_task(fn(*args))
        for fn, args in self._start_ephemeral_tasks:
            connection_tasks.append(fn(*args))

        await asyncio.gather(*connection_tasks)

        post_connection_tasks = []
        for fn, args in self._post_connection_tasks:
            post_connection_tasks.append(fn(*args))

        await asyncio.gather(*post_connection_tasks)

        # if self._post_connect:
        #     await self._post_connect()

    def start(self):
        self._connection_thread = threading.Thread(
            target=self._handle_connections_in_thread
        )
        self._connection_thread.start()

        self._callback_thread = threading.Thread(
            target=self._handle_callbacks_in_thread
        )
        self._callback_thread.start()

    def _handle_connections_in_thread(self):
        self._connection_looper = looper = asyncio.new_event_loop()
        asyncio.set_event_loop(looper)
        looper.set_debug(True)
        looper.run_until_complete(self._start_routine())
        looper.close()

    def _handle_callbacks_in_thread(self):
        self._callback_looper = looper = asyncio.new_event_loop()
        asyncio.set_event_loop(looper)
        looper.set_debug(True)
        looper.run_until_complete(self._process_upstream_messages())
        looper.close()

    def stop(self):
        self._stop = True
        self._callback_looper.call_soon_threadsafe(
            self._callback_tasks.put_nowait, None
        )
        self._connection_looper.call_soon_threadsafe(
            self._connection_tasks.put_nowait, None
        )

    async def _start_routine(self):
        self._looper = asyncio.get_event_loop()
        tasks = []

        await self._connect()
        await asyncio.gather(*tasks)

        while True:
            task = await self._connection_tasks.get()
            if self._stop:
                break
            task()

        for queue in self._downstream_queues.values():
            queue.put_nowait(None)

        close_tasks = []
        for stream in self._downstreams.values():
            close_tasks.append(stream.close())

        for stream in self._upstreams.values():
            close_tasks.append(stream.close())

        asyncio.gather(*close_tasks)
