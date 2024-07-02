from enum import StrEnum
from typing import Any, List, Type, Union, Optional
from pydantic import BaseModel
from pydantic_core import PydanticUndefined
import httpx

from .resource_controller import ResourceController, PendingOperation
from .resource_monitor import ResourceMonitor
from ..itl import Itl
from ..clusters import BaseController, PendingOperation


class Metadata(BaseModel):
    name: str
    fiber: str = "resource"
    remote: Optional[str] = None


def wrap_spec(origCls):
    upstream_cls = None
    have_upstream_default = False
    upstream_default = None

    notification_cls = None
    have_notification_default = False
    notification_default = None

    if "upstream" in origCls.model_fields:
        field = origCls.model_fields["upstream"]
        upstream_cls = field.annotation
        if field.default != PydanticUndefined:
            have_upstream_default = True
            upstream_default = field.default
    else:
        upstream_cls = Optional[Union[ConfigUri, List[ConfigUri]]]
        have_upstream_default = True

    if "notification" in origCls.model_fields:
        field = origCls.model_fields["notification"]
        notification_cls = field.annotation
        if field.default != PydanticUndefined:
            have_notification_default = True
            notification_default = field.default
    else:
        notification_cls = Optional[StreamUri]
        have_notification_default = True

    class SpecValidationClass(BaseModel):
        testConfig: Union[upstream_cls, dict]
        testConfigList: Union[upstream_cls, list]
        testStream: Union[notification_cls, dict]

    testObj = SpecValidationClass(
        testConfig={"configUri": "123"},
        testConfigList=[{"configUri": "123"}],
        testStream={"streamUri": "xyz"},
    )
    if not isinstance(testObj.testConfig, ConfigUri) and not isinstance(
        next(iter(testObj.testConfigList)), ConfigUri
    ):
        raise TypeError("spec.upstream must support type ConfigUri or List[ConfigUri]")
    if not isinstance(testObj.testStream, StreamUri):
        raise TypeError("spec.notification must support type StreamUri")

    class WrappedCls(origCls):
        upstream: upstream_cls
        notification: notification_cls

        if have_upstream_default:
            upstream = upstream_default

        if have_notification_default:
            notification = notification_default

    WrappedCls.__name__ = f"Propagable_{origCls.__name__}"
    WrappedCls.__qualname__ = f"Propagable_{origCls.__qualname__}"
    return WrappedCls


class ConfigUri(BaseModel):
    configUri: str


class StreamUri(BaseModel):
    streamUri: str


class PropagableSpec(BaseModel):
    upstream: ConfigUri
    notification: StreamUri


class PropagableRefs(BaseModel):
    upstream: Optional[Union[ConfigUri, List[ConfigUri]]] = None
    notification: Optional[StreamUri] = None


class PropagableConfig(BaseModel):
    apiVersion: str
    kind: str
    metadata: Metadata
    spec: Optional[PropagableSpec] = None
    refs: Optional[PropagableSpec] = None
    status: Any = None

    def __init_subclass__(cls):
        found_spec = False
        found_refs = False
        for field_name, field_type in cls.__annotations__.items():
            if field_name == "spec":
                cls.__annotations__[field_name] = wrap_spec(field_type)
                found_spec = True
            elif field_name == "refs":
                cls.__annotations__[field_name] = wrap_spec(field_type)
                found_refs = True
        if not found_spec:
            cls.__annotations__["spec"] = Optional[PropagableSpec]
            setattr(cls, "spec", None)
        if not found_refs:
            cls.__annotations__["refs"] = Optional[PropagableRefs]
            setattr(cls, "refs", None)


class PostMessage(StrEnum):
    REQUEST_UPDATE = "request_update"
    PROPAGATE_UPDATE = "propagate_update"
    UPDATE_STREAM_MIGRATED = "update_stream_migrated"


class PropagationPost(BaseModel):
    apiVersion: str
    kind: str
    metadata: Metadata
    request: PostMessage
    notifyStream: Optional[str] = None


def _patch_inplace(itl: Itl, spec, refs, key=""):
    if refs == None:
        return spec
    elif isinstance(refs, str):
        if spec != None and not isinstance(spec, str):
            raise ValueError(
                f"Invalid value in {key}: {refs}. Must be a string to match spec."
            )
        return itl.get_uri(refs)
    elif isinstance(refs, list):
        if spec != None and not isinstance(spec, list):
            raise ValueError(
                f"Invalid value in {key}: {refs}. Must be a list to match spec."
            )
        if spec == None:
            spec = []
        for i, v in enumerate(refs):
            if i >= len(spec):
                spec.append(_patch_inplace(itl, None, v, key + f"[{i}]"))
            else:
                spec[i] = _patch_inplace(
                    itl, spec[i] if i < len(spec) else None, v, key + f"[{i}]"
                )
        return spec
    elif isinstance(refs, dict):
        if spec != None and not isinstance(spec, dict):
            raise ValueError(
                f"Invalid value in {key}: {refs}. Must be a dict to match spec."
            )
        if spec == None:
            spec = {}
        for k, v in refs.items():
            spec[k] = _patch_inplace(
                itl, spec.get(k, None) if spec else None, v, key + f".{k}"
            )
        return spec
    else:
        raise ValueError(
            f"Invalid value in {key}: {refs}. Must be a string, list, or dict"
        )


def _patch_spec(itl, config: dict):
    return _patch_inplace(itl, config.get("spec"), config.get("refs"))


class PropagationController(ResourceController):
    def __init__(
        self,
        itl,
        cluster,
        group,
        version,
        kind,
        fiber,
        config_cls: Type[PropagableConfig],
        update_fn,
        cleanup_fn,
    ):
        super().__init__(itl, cluster, group, version, kind, fiber)
        self.config_cls = config_cls
        self.update = update_fn
        self.cleanup = cleanup_fn

    async def update(self, cluster: str, config: PropagableConfig) -> Any:
        raise NotImplementedError("update_dataset must be implemented by subclass")

    async def cleanup(
        self, old_config: PropagableConfig, new_config: Optional[PropagableConfig]
    ):
        pass

    async def create_resource(self, operation: PendingOperation):
        new_config_dict = await operation.new_config()
        _patch_spec(self.itl, new_config_dict)
        new_config: PropagableConfig = self.config_cls(**new_config_dict)

        # Create the external resource (dataset)
        new_config.status = await self.update(self.cluster, new_config)
        if new_config.spec.notification and new_config.spec.notification.streamUri:
            await self.itl.cluster_post(
                self.cluster,
                {
                    "apiVersion": f"{self.group}/{self.version}",
                    "kind": self.kind,
                    "metadata": new_config.metadata.model_dump(),
                    "request": PostMessage.PROPAGATE_UPDATE,
                    "notifyStream": new_config.spec.notification.streamUri,
                },
            )

        # Accept the config
        return new_config.model_dump(exclude={"refs"})

    async def update_resource(self, operation: PendingOperation):
        old_config: PropagableConfig = self.config_cls(**await operation.old_config())
        new_config_dict = await operation.new_config()
        _patch_spec(self.itl, new_config_dict)
        new_config: PropagableConfig = self.config_cls(
            **new_config_dict, status=old_config.status
        )

        await self.cleanup(old_config, new_config)

        old_notification_uri = None
        new_notification_uri = None
        if old_config.spec.notification:
            old_notification_uri = old_config.spec.notification.streamUri
        if new_config.spec.notification:
            new_notification_uri = new_config.spec.notification.streamUri

        if old_notification_uri != new_notification_uri:
            if old_notification_uri:
                await self.itl.cluster_post(
                    self.cluster,
                    {
                        "apiVersion": new_config.apiVersion,
                        "kind": new_config.kind,
                        "metadata": new_config.metadata.model_dump(),
                        "request": PostMessage.UPDATE_STREAM_MIGRATED,
                        "notifyStream": old_config.spec.notification.streamUri,
                    },
                )

        # Create the external resource (dataset)
        new_config.status = await self.update(self.cluster, new_config)
        if new_config.spec.notification.streamUri:
            await self.itl.cluster_post(
                self.cluster,
                {
                    "apiVersion": f"{self.group}/{self.version}",
                    "kind": self.kind,
                    "metadata": new_config.metadata.model_dump(),
                    "request": PostMessage.PROPAGATE_UPDATE,
                    "notifyStream": new_config.spec.notification.streamUri,
                },
            )

        # Accept the config
        return new_config.model_dump(exclude={"refs"})

    async def delete_resource(self, operation: PendingOperation):
        old_config: PropagableConfig = self.config_cls(**await operation.old_config())

        if old_config.spec.notification.streamUri:
            await self.itl.cluster_post(
                self.cluster,
                {
                    "apiVersion": old_config.apiVersion,
                    "kind": old_config.kind,
                    "metadata": old_config.metadata.model_dump(),
                    "request": PostMessage.UPDATE_STREAM_MIGRATED,
                    "notifyStream": old_config.spec.notification.streamUri,
                },
            )

        await self.cleanup(old_config, None)

    async def post_resource(self, operation: PendingOperation):
        config = PropagationPost(**await operation.message())
        if config.request == PostMessage.REQUEST_UPDATE:
            old_config: PropagableConfig = self.config_cls(
                **await operation.old_config()
            )
            await self.update(self.cluster, old_config)
            if old_config.spec.notification and old_config.spec.notification.streamUri:
                await self.itl.cluster_post(
                    self.cluster,
                    {
                        "apiVersion": f"{self.group}/{self.version}",
                        "kind": self.kind,
                        "metadata": old_config.metadata.model_dump(),
                        "request": PostMessage.PROPAGATE_UPDATE,
                        "notifyStream": old_config.spec.notification.streamUri,
                    },
                )
        elif config.request == PostMessage.PROPAGATE_UPDATE:
            self.itl.stream_post(
                url=config.notifyStream, message={"dataset_update": True}
            )
        elif config.request == PostMessage.UPDATE_STREAM_MIGRATED:
            self.itl.stream_post(
                url=config.notifyStream, message={"refresh_update_stream": True}
            )
        else:
            raise ValueError("Invalid request type")


class MonitorState(BaseModel):
    updateStream: Optional[str] = None
    metadata: Metadata
    upstream: Any = None


class PropagationMonitor(ResourceMonitor):
    def __init__(
        self, itl, cluster, group, version, kind, fiber, config_cls: PropagableConfig
    ):
        super().__init__(itl, cluster, group, version, kind, fiber)
        self.config_cls = config_cls
        self.running_listeners = {}

    def _pre_migrate_update_stream(self, identifier, new_config: PropagableConfig):
        old_upstream_link = None
        old_config: MonitorState = self.get(new_config.metadata.name)
        if old_config:
            old_upstream_link = old_config.upstream
        old_update_stream = self.running_listeners.get(identifier, None)

        new_upstream_link = new_config.spec.upstream
        if new_upstream_link and isinstance(new_upstream_link, ConfigUri):
            new_upstream_dict = httpx.get(new_upstream_link.configUri).json()
            if new_upstream_dict == None:
                new_update_stream = old_update_stream
            else:
                new_upstream: PropagableConfig = self.config_cls(
                    **new_upstream_dict,
                )
                new_update_stream = new_upstream.spec.notification.streamUri
        else:
            new_update_stream = None

        if (old_upstream_link != new_upstream_link) or (
            new_update_stream != old_update_stream
        ):
            self.itl.stream_detach(identifier)
            if identifier in self.running_listeners:
                del self.running_listeners[identifier]

        return new_update_stream

    async def onput(self, config):
        # This is written like a quine since messages might trigger a migration to a new stream,
        # and migration is the same as calling onput again.

        new_config: PropagableConfig = self.config_cls(**config)
        identifier = "PropagationMonitor" + "/".join(
            [
                self.cluster,
                self.group,
                self.version,
                self.kind,
                new_config.metadata.name,
                new_config.metadata.fiber or "resource",
            ]
        )

        new_update_stream = self._pre_migrate_update_stream(identifier, new_config)

        async def post_update_request(
            *args, dataset_update=False, refresh_update_stream=False, **kwargs
        ):
            if dataset_update:
                await self.itl.cluster_post(
                    self.cluster,
                    {
                        "apiVersion": f"{self.group}/{self.version}",
                        "kind": self.kind,
                        "metadata": new_config.metadata.model_dump(),
                        "request": PostMessage.REQUEST_UPDATE,
                    },
                )

            if refresh_update_stream:
                new_update_stream = self._pre_migrate_update_stream(
                    identifier, new_config
                )

                if new_update_stream and identifier not in self.running_listeners:
                    self.running_listeners[identifier] = new_update_stream
                    self.itl.stream_attach(
                        post_update_request,
                        stream=new_update_stream,
                        onconnect=post_update_request,
                        key=identifier,
                    )

        if new_update_stream and identifier not in self.running_listeners:
            self.running_listeners[identifier] = new_update_stream
            self.itl.stream_attach(
                post_update_request,
                stream=new_update_stream,
                onconnect=post_update_request,
                key=identifier,
            )

        return MonitorState(
            updateStream=new_update_stream,
            metadata=new_config.metadata,
            upstream=new_config.spec.upstream,
        )

    async def ondelete(self, resource: MonitorState):
        identifier = "PropagationMonitor" + "/".join(
            [
                self.cluster,
                self.group,
                self.version,
                self.kind,
                resource.metadata.name,
                resource.metadata.fiber or "resource",
            ]
        )
        self.itl.stream_detach(identifier)


class PropagationOperator:
    CONFIG_CLS: Type[PropagableConfig]

    def __init_subclass__(cls) -> None:
        if cls.CONFIG_CLS is None:
            raise ValueError(
                "CONFIG_CLS must be set to a subclass of PropagationOperator"
            )

        if not issubclass(cls.CONFIG_CLS, PropagableConfig):
            raise TypeError("CONFIG_CLS must be a subclass of PropagationOperator")

    async def update(self, cluster: str, config: PropagableConfig) -> Any:
        raise NotImplementedError("update must be implemented by subclass")

    async def cleanup(
        self, old_config: PropagableConfig, new_config: Optional[PropagableConfig]
    ):
        pass

    def __init__(self, itl, cluster, group, version, kind, fiber="resource"):
        self.itl = itl
        self.controller = PropagationController(
            itl,
            cluster,
            group,
            version,
            kind,
            fiber,
            self.CONFIG_CLS,
            self.update,
            self.cleanup,
        )
        self.monitor = PropagationMonitor(
            itl, cluster, group, version, kind, fiber, self.CONFIG_CLS
        )
        self.cluster = cluster
        self.group = group
        self.version = version
        self.kind = kind
        self.fiber = fiber

    def start_controller(self):
        self.controller.start(self)

    def start_monitor(self):
        self.monitor.start(self)

    def start_manual_updates(self):
        @self.itl.controller(
            self.cluster,
            self.group,
            self.version,
            self.kind,
            fiber=self.fiber,
            validate=False,
        )
        async def accept_all(pending: BaseController):
            async for op in pending:
                print("Accepting", op.data)
                await op.accept()
