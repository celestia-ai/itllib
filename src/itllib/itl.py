import atexit
import asyncio
from collections import defaultdict
from contextvars import Context
import inspect
import os
import threading
import time
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
        self._data_handlers = defaultdict(lambda: defaultdict(list))
        self._controllers = {}

        # Async stuff
        self._connection_thread = None
        self._connection_looper = None
        self._connection_tasks = asyncio.Queue()
        self._callback_thread = None
        self._callback_context = None
        self._callback_looper = None
        self._callback_tasks = asyncio.Queue()
        self._disconnect_tasks = []

        self._ready_looper = None
        self._finished_looper = None
        self.ready_queue = asyncio.Queue()
        self.finished_queue = asyncio.Queue()

        self._started = False
        self._stopped = False

        # Resources
        self._secrets = {}
        self._streams: dict[str, StreamOperations] = {}
        self._buckets = {}
        self._piles = {}
        self._clusters: dict[str, ClusterOperations] = {}
        self._loops: dict[str, LoopOperations] = {}

        # Stream interactions
        self._start_persistent_tasks = {}
        self._downstream_queues = defaultdict(asyncio.Queue)
        self._started_streams = set()
        self._start_ephemeral_tasks = []

        self._resource_pile = ResourcePile()
        self._resolver = ResourceResolver(self._resource_pile)
        self._keys = defaultdict(lambda: None)

        self._apply_config(*configs, client=client)
        atexit.register(self.stop)

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
    
    # def __getitem__(self, kindName):
    #     return self._resolver.get_resource_by_reference(ResourceReference(*kindName))

    def _object_download(self, pile, key=None, notification=None, attach_prefix=False):
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

    def _object_upload(
        self, pile, key, file_descriptor, metadata={}, attach_prefix=False
    ):
        pile_ops = self._piles[pile]

        if attach_prefix:
            key = f"{pile_ops.prefix or ''}{key}"

        return pile_ops.put(key, file_descriptor, metadata)

    def _object_delete(self, pile, key=None, attach_prefix=False):
        pile_ops = self._piles[pile]

        if attach_prefix:
            key = f"{pile_ops.prefix or ''}{key}"

        return pile_ops.delete(key)

    async def cluster_create(self, cluster, data):
        return await self._clusters[cluster].create_resource(data)

    async def cluster_read_all(
        self, from_cluster, cluster=None, group=None, version=None, kind=None, name=None, fiber=None, utctime=None
    ):
        return await self._clusters[from_cluster].read_all_resources(
            group, version, kind, name, fiber, utctime, cluster=cluster
        )

    async def cluster_read(self, cluster, group, version, kind, name, fiber=None):
        return await self._clusters[cluster].read_resource(group, version, kind, name, fiber)
    
    async def cluster_read_queue(self, from_cluster, cluster=None, group=None, version=None, kind=None, name=None, fiber=None):
        return await self._clusters[from_cluster].read_queue(group, version, kind, name, fiber, cluster=cluster)

    async def cluster_patch(self, cluster, data):
        return await self._clusters[cluster].patch_resource(data)

    async def cluster_update(self, cluster, data):
        return await self._clusters[cluster].update_resource(data)

    async def cluster_apply(self, cluster, data):
        return await self._clusters[cluster].apply_resource(data)
    
    async def cluster_post(self, from_cluster, data, cluster=None):
        return await self._clusters[from_cluster].post_resource(data, cluster=cluster)

    async def cluster_delete(self, from_cluster, group, version, kind, name, fiber=None, cluster=None):
        return await self._clusters[from_cluster].delete_resource(group, version, kind, name, fiber, cluster=cluster)
    
    async def cluster_unlock(self, cluster, group, version, kind, name, fiber=None, from_cluster=None):
        return await self._clusters[cluster].unlock_resource(group, version, kind, name, fiber, from_cluster=from_cluster)

    def cluster_controller(self, from_cluster, group, version, kind, name, fiber=None, validate=True, cluster=None):
        cluster_obj = self._clusters[from_cluster]
        return cluster_obj.control_resource(
            cluster or from_cluster, group, version, kind, name, fiber, validate=validate
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

    def ondata(self, stream=None, loop=None, streamId=None, key=None):
        if stream == None:
            loop_info = self._loops[loop]
            stream = f"stream/{loop}/{streamId}"
            if stream not in self._streams:
                self._streams[stream] = StreamOperations(
                    loop_info.connect_info.stream_connection_info(streamId),
                    loop_info.apikey,
                )

        self._ensure_stream_connection([stream])

        def decorator(func):
            self._data_handlers[stream][key].append(func)
            return func

        return decorator

    def controller(
        self,
        cluster,
        group=None,
        version=None,
        kind=None,
        name=None,
        fiber='resource',
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
                    event["fiber"],
                    validate=validate,
                    cluster=event["cluster"],
                )
                try:
                    async with operations:
                        try:
                            await func(operations)
                        except Exception as e:
                            print(
                                f"Error in controller {func.__name__}: {traceback.format_exc()}"
                            )
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
                if name and event["name"] != name:
                    return
                if fiber and event["fiber"] != fiber:
                    return

                asyncio.create_task(controller_wrapper(*args, **event))

            self._controllers.setdefault(cluster, []).append(func)

            async def check_queue():
                for queued_op in await self.cluster_read_queue(cluster, group=group, version=version, kind=kind, name=name, fiber=fiber):
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

    def ondisconnect(self, func):
        if self._stopped:
            raise RuntimeError(
                "Cannot add ondisconnect handler after itl has been stopped"
            )
        else:
            self._disconnect_tasks.append(func)

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
            for collection in self._data_handlers[identifier].values():
                for handler in collection:
                    if isinstance(message, dict):
                        args = []
                        kwargs = message
                    else:
                        args = [message]
                        kwargs = {}

                    exec_callback(handler, args, kwargs)

        for fn, args in self._start_ephemeral_tasks:
            asyncio.create_task(fn(*args))

        self._ready_looper.call_soon_threadsafe(self.ready_queue.put_nowait, None)

        while True:
            task = await self._callback_tasks.get()

            if self._stopped:
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

        for fn in self._disconnect_tasks:
            fn()

        self._finished_looper.call_soon_threadsafe(self.finished_queue.put_nowait, None)

    async def _post_stream_message(self, url, message):
        # call HTTP POST on key, passing message as data
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json.dumps(message)) as response:
                response.raise_for_status()

    def stream_send(self, stream=None, message=None, loop=None, streamId=None):
        if stream == None:
            loop_info = self._loops[loop]
            stream = f"stream/{loop}/{streamId}"
            if stream not in self._streams:
                self._streams[stream] = StreamOperations(
                    loop_info.connect_info.stream_connection_info(streamId),
                    loop_info.apikey,
                )

        self._ensure_stream_connection([stream])

        if self._connection_looper:
            self._connection_looper.call_soon_threadsafe(
                self._downstream_queues[stream].put_nowait, message
            )
        else:
            task = self._downstream_queues[stream].put, (message,)
            self._connection_looper.call_soon_threadsafe(
                self._downstream_queues[stream].put_nowait, task
            )

    def stream_send_sync(self, stream, message):
        if self._started and self._stopped:
            raise RuntimeError("Cannot send messages after itl has been stopped")

        self._ensure_stream_connection([stream])

        if self._connection_looper:
            self._downstream_queues[stream].put_nowait(message)
        else:
            task = self._downstream_queues[stream].put, (message,)
            self._downstream_queues[stream].put_nowait(task)

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

            if self._stopped:
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

        while not self._stopped:
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
                        if self._stopped:
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

            if self._stopped:
                return

            # Backoff before reconnecting
            backoff_time = await self._exponential_backoff(backoff_time)

    async def _exponential_backoff(self, current_backoff_time):
        """Sleeps the process for an exponential backoff time."""
        await asyncio.sleep(2**current_backoff_time)
        # Return the next backoff time, capped at 2**7 seconds
        return min(current_backoff_time + 1, 7)

    def start(self, daemon=True):
        if self._started:
            if self._stopped:
                raise RuntimeError("Cannot start the same itl twice")
            return
        self._started = True
        self._ready_looper = asyncio.new_event_loop()
        self._ready_looper.set_debug(True)
        self._ready_looper.set_exception_handler(self.default_exception_handler)

        self._finished_looper = asyncio.new_event_loop()
        self._finished_looper.set_debug(True)
        self._finished_looper.set_exception_handler(self.default_exception_handler)

        self._connection_thread = threading.Thread(
            target=self._handle_connections_in_thread, daemon=daemon
        )
        self._connection_thread.start()

        self._callback_thread = threading.Thread(
            target=self._handle_callbacks_in_thread, daemon=daemon
        )
        self._callback_thread.start()

        self._ready_looper.run_until_complete(self.ready_queue.get())
        self._ready_looper.run_until_complete(self.ready_queue.get())

    def _handle_connections_in_thread(self):
        self._connection_looper = looper = asyncio.new_event_loop()
        asyncio.set_event_loop(looper)
        looper.set_debug(True)
        looper.set_exception_handler(self.default_exception_handler)
        looper.run_until_complete(self._start_routine())

    def _handle_callbacks_in_thread(self):
        self._callback_looper = looper = asyncio.new_event_loop()
        self._callback_context = Context()
        asyncio.set_event_loop(looper)
        looper.set_debug(True)
        looper.set_exception_handler(self.default_exception_handler)
        looper.run_until_complete(self._process_upstream_messages())

    def stop(self):
        if not self._started:
            print("Warning: itl was never started")
            return
        if self._stopped:
            return
        self._stopped = True

        if self._callback_looper == None or self._connection_looper == None:
            print("Warning: itl did not finish starting")
            return

        self._callback_looper.call_soon_threadsafe(
            self._callback_tasks.put_nowait, None
        )
        self._callback_thread.join()

        self._connection_looper.call_soon_threadsafe(
            self._connection_tasks.put_nowait, None
        )
        self._connection_thread.join()

    def wait(self):
        try:
            self._finished_looper.run_until_complete(self.async_wait())
            self._finished_looper.close()
        except:
            self.stop()

    async def async_wait(self):
        try:
            await self.finished_queue.get()
            await self.finished_queue.get()
        except:
            pass

    async def _start_routine(self):
        self._looper = asyncio.get_event_loop()

        for fn, args in self._start_persistent_tasks.values():
            asyncio.create_task(fn(*args))

        self._ready_looper.call_soon_threadsafe(self.ready_queue.put_nowait, None)

        while True:
            task = await self._connection_tasks.get()
            if self._stopped:
                break
            task()

        for queue in self._downstream_queues.values():
            queue.put_nowait(None)

        close_tasks = []
        for stream in self._streams.values():
            if stream.socket:
                close_tasks.append(stream.socket.close())

        await asyncio.gather(*close_tasks)
        self._finished_looper.call_soon_threadsafe(self.finished_queue.put_nowait, None)

    def default_exception_handler(self, loop, context):
        # context["message"] will contain the error message
        # context["exception"] will contain the actual exception object
        if "exception" not in context:
            return

        exception = context["exception"]

        # Python uses a Runtime error to signal that the event loop is closed, so
        # unfortunately there's not a great way to check for it. This is an ugly
        # workaround.
        if isinstance(exception, RuntimeError):
            if (
                str(exception)
                == "cannot schedule new futures after interpreter shutdown"
            ):
                error_dir = os.path.dirname(
                    exception.__traceback__.tb_frame.f_code.co_filename
                )
                current_dir = os.path.dirname(os.path.abspath(__file__))
                if error_dir == current_dir:
                    print(
                        "Unexpected shutdown. Make sure to call itl.stop() before exiting the program."
                    )

        else:
            print(f"Exception: {context}")
