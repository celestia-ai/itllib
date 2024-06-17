from .. import Itl
from ..clusters import BaseController


class ResourceUpdater:
    def __init__(
        self,
        itl: Itl,
        cluster: str,
        group: str,
        version: str,
        kind: str,
        resource_fiber: str = "resource",
        updater_fiber="updater",
    ):
        self.itl: Itl = itl
        self.cluster = cluster
        self.group = group
        self.version = version
        self.kind = kind
        self.resource_fiber = resource_fiber
        self.updater_fiber = updater_fiber
        self._started = False

    def start(self, key=None):
        if self._started:
            return
        self._started = True

        # Start the updater
        self.itl.controller(
            self.cluster,
            self.group,
            self.version,
            self.kind,
            fiber=self.updater_fiber,
            key=key,
        )(self._handle_updater_config)

        # Start watching for changes
        self.itl.onupdate(
            self.cluster,
            group=self.group,
            version=self.version,
            fiber=self.resource_fiber,
            key=key,
            onconnect=self._load_existing,
        )(self._handle_resource_event)

        # Load the existing resource
        self._load_existing()
