import asyncio
from collections import defaultdict
import inspect
import threading
import typing
from glob import glob

import websockets
import json
import aiohttp
import yaml

from .piles import BucketOperations, PileOperations
from .clusters import DatabaseOperations, ClusterOperations
from .loops import LoopOperations, StreamOperations


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


def collect_secrets(secrets_dir):
    bucket_keys = {}
    result = {}
    for path in glob(f"{secrets_dir}/*.json"):
        with open(path) as inp:
            secret_data = json.load(inp)

        secret_name = secret_data["metadata"]["name"]
        if secret_name in bucket_keys:
            raise ValueError(f"Duplicate key name: {secret_name}")

        result[secret_name] = secret_data

    for path in glob(f"{secrets_dir}/*.yaml"):
        with open(path) as inp:
            secret_data = yaml.safe_load(inp)

        secret_name = secret_data["metadata"]["name"]
        if secret_name in bucket_keys:
            raise ValueError(f"Duplicate key name: {secret_name}")

        result[secret_name] = secret_data

    return result


class Itl:
    def __init__(self) -> None:
        self._messages = asyncio.Queue()

        # User-specified handlers
        self._data_handlers = {}
        self._trigger_handlers = {}
        self._controllers = {}
        self._requires_start = {}
        self._main = None
        self._post_connect = None

        # Async stuff
        self._thread = None
        self._looper = None
        self._message_tasks = []

        # Old stuff, to be removed
        self._stop = False

        # Resources
        self._secrets = {}
        self._streams = {}
        self._buckets = {}
        self._piles = {}
        self._databases = {}
        self._clusters: dict[str, ClusterOperations] = {}
        self._loops: dict[str, LoopOperations] = {}

        # Stream interactions
        self._downstreams = {}
        self._upstreams = {}
        self._upstream_tasks = {}
        self._downstream_tasks = {}
        self._downstream_queues = defaultdict(asyncio.Queue)
        self._connection_tasks = {}
        self._started_streams = set()

    def apply_config(self, config, secrets):
        if isinstance(config, str):
            with open(config) as inp:
                config = yaml.safe_load(inp)

        self.apply_secrets(secrets)
        self.update_loops(config.get("loops", []))
        self.update_streams(config.get("streams", []))
        self.update_buckets(config.get("buckets", []))
        self.update_piles(config.get("piles", []))
        self.update_databases(config.get("databases", []))
        self.update_clusters(config.get("clusters", []))

    def apply_secrets(self, secrets_dir):
        # TODO: update affected resources
        self._secrets.update(collect_secrets(secrets_dir))

        for secretName, secret in self._secrets.items():
            if secret["apiVersion"] != "itllib/v1":
                continue
            if "spec" not in secret:
                continue

            kind = secret["kind"]
            spec = secret["spec"]

            if kind == "LoopSecret":
                loopName = secret["metadata"]["name"]
                self._loops[loopName] = LoopOperations(spec)
            elif kind == "DatabaseSecret":
                clusterName = secret["metadata"]["name"]
                self._databases[clusterName] = DatabaseOperations(secret)
            elif kind == "BucketSecret":
                bucketName = secret["metadata"]["name"]
                self._buckets[bucketName] = BucketOperations(spec)

    def update_loops(self, loops):
        for loop in loops:
            name = loop["name"]
            secret = loop["secret"]
            self._loops[name] = LoopOperations(self._secrets[secret]["spec"])

    def update_streams(self, streams):
        for stream in streams:
            name = stream["name"]
            loop = stream["loop"]
            key = stream.get("key", name)
            group = stream.get("group", None)
            self._streams[name] = StreamOperations(
                self._loops[loop], key=key, group=group
            )

    def update_buckets(self, buckets):
        for bucket in buckets:
            name = bucket["name"]
            key = bucket["secret"]
            self._buckets[name] = BucketOperations(self._secrets[key]["spec"])

    def update_piles(self, piles):
        for pile in piles:
            name = pile["name"]
            bucket = pile["bucket"]
            prefix = pile.get("prefix", None)
            pattern = pile.get("pattern", None)
            self._piles[name] = PileOperations(
                self._buckets[bucket], prefix=prefix, pattern=pattern
            )

    def update_databases(self, databases):
        for database in databases:
            name = database["name"]
            secret = database["secret"]
            self._databases[name] = DatabaseOperations(self._secrets[secret])

    def update_clusters(self, clusters):
        for config in clusters:
            name = config["name"]
            database = config["database"]
            stream = config.get("eventStream", None)
            self._clusters[name] = ClusterOperations(self._databases[database], stream)

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

    async def resource_create(self, cluster, data):
        return await self._clusters[cluster].create_resource(data)
        # Remember to update the underlying functions
        # To call the REST APIs rather than the database directly

    async def resource_read(self, cluster, name):
        return await self._clusters[cluster].read_resource(name)

    async def resource_patch(self, cluster, data):
        return await self._clusters[cluster].patch_resource(data)

    async def resource_update(self, cluster, data):
        return await self._clusters[cluster].update_resource(data)

    async def resource_apply(self, cluster, data):
        return await self._clusters[cluster].apply_resource(data)

    async def resource_delete(self, cluster, group, version, kind, name):
        return await self._clusters[cluster].delete_resource(group, version, kind, name)

    def resource_controller(
        self, cluster, group, version, kind, name=None, validate=False
    ):
        cluster_obj = self._clusters[cluster]
        return cluster_obj.control_resource(
            group, version, kind, name, validate=validate
        )
        # yield controller

    def get_url(self, identifier):
        if identifier in self._streams:
            return self._streams[identifier].connect_url
        else:
            return identifier

    def downstreams(self, streams):
        """
        Update the downstream tasks based on the provided streams. If a downstream task for a
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
            if identifier in self._downstream_tasks:
                continue

            # Check if the Itl is already running
            if not self._looper:
                task = self._attach_stream, (identifier,)
                self._downstream_tasks[identifier] = task
            else:
                self._looper.call_soon_threadsafe(
                    self._schedule_downstream_task, identifier
                )

    def _schedule_downstream_task(self, identifier):
        """
        Schedules a downstream task for the given identifier if it doesn't already exist.

        Args:
        - identifier (str): Identifier for the stream.

        Returns:
        None
        """
        if identifier in self._downstream_tasks:
            return

        task = self._attach_stream(identifier)
        asyncio.create_task(task)
        self._downstream_tasks[identifier] = task

    def upstreams(self, streams):
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
            if identifier in self._upstream_tasks:
                continue

            # Check if the Itl is already running
            if not self._looper:
                task = self._attach_stream, (identifier,)
                self._upstream_tasks[identifier] = task
            else:
                self._looper.call_soon_threadsafe(
                    self._schedule_upstream_task, identifier
                )

    def _schedule_upstream_task(self, identifier):
        """
        Schedules an upstream task for the given identifier if it doesn't already exist.

        Args:
        - identifier (str): Identifier for the stream.

        Returns:
        None
        """
        if identifier in self._upstream_tasks:
            return

        task = self._attach_stream(identifier)
        asyncio.create_task(task)
        self._upstream_tasks[identifier] = task

    def ondata(self, stream):
        if stream not in self._upstreams:
            self.upstreams([stream])

        def decorator(func):
            self._data_handlers.setdefault(stream, []).append(func)
            return func

        return decorator

    def ontrigger(self, stream):
        def decorator(func):
            self._trigger_handlers.setdefault(stream, []).append(func)

        return decorator

    def controller(self, cluster, group=None, version=None, kind=None, validate=False):
        stream = self._clusters[cluster].stream
        database = self._clusters[cluster].database.name
        self.upstreams([stream])

        def decorator(func):
            async def controller_wrapper(*args, **event):
                operations = self.resource_controller(
                    cluster,
                    event["group"],
                    event["version"],
                    event["kind"],
                    validate=validate,
                )
                async with operations:
                    self._requires_start[func] = True
                    await func(operations)
                    print("done")

            @self.ondata(stream)
            async def event_handler(*args, **event):
                if event["database"] != database:
                    return
                if group != None and event["group"] != group:
                    return
                if version != None and event["version"] != version:
                    return
                if kind != None and event["kind"] != kind:
                    return

                requires_start = self._requires_start.get(func, True)
                self._requires_start[func] = False
                if requires_start:
                    asyncio.create_task(controller_wrapper(*args, **event))

            self._controllers.setdefault(cluster, []).append(func)
            return func

        return decorator

    def onstart(self, func):
        self._main = func
        return func

    def onconnect(self, func):
        self._post_connect = func
        return func

    async def handle_request(self, scope):
        # TODO: This isn't used right now. Update it so accepts a parameter dictionary directly.
        """
        Handle an incoming request by matching the scope to a route and executing the corresponding
        function. Check that the method is supported, then parse the path and match it to a route.
        Once a route is matched, validate and update the arguments and call the corresponding function.

        :param scope: The request scope containing type, method, path, and data.
        :param receive: The receive callable for the ASGI application.
        :param send: The send callable for the ASGI application.
        :return: The result of the matched function, or None if no match is found.
        :raises ValueError: If the scope type is not 'itl' or the method is unsupported.
        """

        method = scope["method"]
        if method not in self.routes:
            raise ValueError(
                "Unsupported method, must be one of: " + ", ".join(self.routes.keys())
            )

        # Parse the path and prepare the path parameters
        request_path = scope["path"]
        path_params = None

        # Iterate through the routes to find a matching route
        for route_pattern, route_func, expected_params, type_hints in self.routes[
            method
        ]:
            pattern_parts = route_pattern.split("/")
            path_parts = request_path.split("/")

            is_match = True
            extracted_params = {}

            if len(pattern_parts) != len(path_parts):
                continue

            # Match each part of the candidate route to the request path
            for pattern_part, path_part in zip(pattern_parts, path_parts):
                if pattern_part.startswith("{") and pattern_part.endswith("}"):
                    extracted_params[pattern_part[1:-1]] = path_part
                elif pattern_part != path_part:
                    is_match = False
                    break

            if not is_match:
                continue

            # Convert the values based on type hints
            for key, value in extracted_params.items():
                if key in type_hints:
                    extracted_params[key] = type_hints[key](value)

            for key, value in scope.get("data", {}).items():
                if key in type_hints:
                    scope["data"][key] = type_hints[key](value)

            scope["path_params"] = extracted_params

            # Check if the arguments match the function signature
            unexpected_args = extracted_params.keys() - expected_params
            if unexpected_args:
                print(f"Unexpected arguments: {', '.join(unexpected_args)}")
                return False

            await route_func(scope)
            return

        print("No match for scope", scope)

    async def _process_upstream_messages(self):
        while not self._stop:
            # TODO: check all incoming streams for messages

            # empty the queue
            while not self._messages.empty():
                try:
                    await self._messages.get()
                except asyncio.CancelledError:
                    pass

            if self._stop:
                break

            # otherwise, wait for a message
            try:
                await self._messages.get()
            except asyncio.CancelledError:
                pass

        for queue in self._downstream_queues.values():
            queue.put_nowait(None)

        for stream in self._downstreams.values():
            await stream.close()

        for stream in self._upstreams.values():
            await stream.close()

    async def stream_send(self, key, message):
        if key not in self._streams:
            # call HTTP POST on key, passing message as data
            async with aiohttp.ClientSession() as session:
                async with session.post(key, data=json.dumps(message)) as response:
                    response.raise_for_status()
            return

        if key not in self._downstream_tasks:
            self.downstreams([key])

        if self._looper:
            self._looper.call_soon_threadsafe(
                self._downstream_queues[key].put_nowait, message
            )
        else:
            task = self._downstream_queues[key].put, (message,)
            self._message_tasks.append(task)

    def _requeue(self, identifier, message):
        if message == None:
            return

        old_queue = self._downstream_queues[identifier]
        new_queue = asyncio.Queue()
        if message != None:
            new_queue.put_nowait(message)
        while not old_queue.empty():
            new_queue.put_nowait(old_queue.get_nowait())

        self._downstream_tasks[identifier] = new_queue

    async def _attach_stream(self, identifier):
        if identifier in self._started_streams:
            return

        self._started_streams.add(identifier)
        state = Namespace()
        state.message = None

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

            self._messages.put_nowait(None)

            message = json.loads(serialized_data)
            # TODO: Run all handlers in parallel
            for handler in self._data_handlers[identifier]:
                if isinstance(message, dict):
                    args = []
                    kwargs = message
                else:
                    args = [message]
                    kwargs = {}

                if inspect.iscoroutinefunction(handler):
                    asyncio.create_task(handler(*args, **kwargs))
                else:
                    handler(*args, **kwargs)

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

                ws_url = self.get_url(identifier)
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

                            if completed == tasks[0]:
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
                print("stopping, completed")
                return

            # Backoff before reconnecting
            backoff_time = await self._exponential_backoff(backoff_time)

    async def _exponential_backoff(self, current_backoff_time):
        """Sleeps the process for an exponential backoff time."""
        await asyncio.sleep(2**current_backoff_time)
        # Return the next backoff time, capped at 2**7 seconds
        return min(current_backoff_time + 1, 7)

    async def _connect(self):
        tasks = []
        for fn, args in self._upstream_tasks.values():
            tasks.append(fn(*args))
        for fn, args in self._downstream_tasks.values():
            tasks.append(fn(*args))
        for fn, args in self._message_tasks:
            tasks.append(fn(*args))

        await asyncio.gather(*tasks)

        if self._post_connect:
            await self._post_connect()

    def start_thread(self):
        self._thread = threading.Thread(target=self._run_in_thread)
        self._thread.start()

    def join(self):
        self._thread.join()

    def _run_in_thread(self):
        self._looper = looper = asyncio.new_event_loop()
        asyncio.set_event_loop(looper)
        asyncio.get_event_loop().set_debug(True)
        looper.run_until_complete(main(self))
        looper.close()

    def stop_thread(self):
        self.stop_itl()

    def stop_itl(self):
        self._stop = True
        self._looper.call_soon_threadsafe(self._messages.put_nowait, None)
        self._looper = None


async def main(itl, args=None):
    process_messages_task = itl._process_upstream_messages()

    async def start_routine():
        itl._looper = asyncio.get_event_loop()
        await itl._connect()

        if itl._main:
            await itl._main()

    await asyncio.gather(process_messages_task, start_routine())
