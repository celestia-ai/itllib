import asyncio
from typing import Any, Dict, Type
from pydantic import BaseModel
from itllib.clusters import PendingOperation


class TrialSpec(BaseModel):
    type: str = "new_trial"
    parameters: Dict[str, Any]


class OptimizerMixin:
    SYNC = False
    TRIAL_FIBER = "trials"
    CONFIG_CLS: Type[BaseModel] = None
    TRIAL_CLS: Type[BaseModel] = TrialSpec

    async def handle_feedback(self, config, parameters, metrics):
        raise NotImplementedError

    async def handle_trial_request(self, config, parameters):
        raise NotImplementedError

    async def post_resource(self, op: PendingOperation):
        message_json = await op.message()
        config = await op.old_config()

        if self.CONFIG_CLS:
            config = self.CONFIG_CLS(**config)

        message_type = message_json["spec"].get("type")

        if message_type == "get_trial":
            request = message_json["spec"].get("parameters")

            async def submit_trial(config, request):
                parameters = await self.handle_trial_request(config, request)

                if parameters == None:
                    return

                trial = self.TRIAL_CLS(**parameters).model_dump()

                await self.itl.cluster_post(
                    self.cluster,
                    data={
                        "apiVersion": op.api_version,
                        "kind": op.kind,
                        "metadata": {
                            "name": op.name,
                            "fiber": self.TRIAL_FIBER,
                        },
                        "spec": trial,
                    },
                )

            if self.SYNC:
                await submit_trial(config, request)
            else:
                asyncio.create_task(submit_trial(config, request))

        elif message_type == "post_feedback":
            parameters = message_json["spec"].get("parameters")
            metrics = message_json["spec"].get("metrics")

            if self.SYNC:
                await self.handle_feedback(
                    config,
                    parameters,
                    metrics,
                )
            else:
                asyncio.create_task(self.handle_feedback(config, parameters, metrics))


class FeedbackSpec(BaseModel):
    type: str = "post_feedback"
    parameters: Dict[str, Any]
    metrics: Dict[str, Any]


class ExperimenterMixin:
    SYNC = False
    FEEDBACK_FIBER = "feedback"
    CONFIG_CLS: Type[BaseModel] = None
    FEEDBACK_CLS: Type[BaseModel] = FeedbackSpec

    async def handle_trial(self, config, parameters: dict):
        raise NotImplementedError

    async def post_resource(self, op: PendingOperation):
        message = await op.message()
        print("handling trial", message)
        if self.CONFIG_CLS:
            config = self.CONFIG_CLS(**await op.old_config())
        else:
            config = await op.old_config()

        parameters = message["spec"].get("parameters")

        async def submit_feedback(config, parameters):
            metrics = await self.handle_trial(config, parameters)

            result = self.FEEDBACK_CLS(parameters=parameters, metrics=metrics)

            if not self.SYNC:
                # The config might have changed by the time we get here
                config = await self.itl.cluster_get(
                    self.cluster,
                    op.group,
                    op.version,
                    op.kind,
                    op.name,
                    op.fiber,
                )

            await self.itl.cluster_post(
                self.cluster,
                data={
                    "apiVersion": op.api_version,
                    "kind": op.kind,
                    "metadata": {
                        "name": op.name,
                        "fiber": self.FEEDBACK_FIBER,
                    },
                    "spec": result.model_dump(),
                },
            )

        if self.SYNC:
            await submit_feedback(config, parameters)
        else:
            asyncio.create_task(submit_feedback(config, parameters))
