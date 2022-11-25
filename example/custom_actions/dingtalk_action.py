# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/24.
    Description:
    Changelog: all notable changes to this file will be documented
"""
import json
import logging
from typing import Optional
from pydantic import BaseModel
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext


class DingTalkConfig(BaseModel):
    # Whether to print the message in upper case.
    is_at_all: Optional[bool]
    token:Optional[str]


# A basic example of a DataHub action that prints all
# events received to the console.
class DingTalkAction(Action):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = DingTalkConfig.parse_obj(config_dict or {})
        return cls(action_config, ctx)

    def __init__(self, config:DingTalkConfig, ctx: PipelineContext):
        self.config = config

    def act(self, event: EventEnvelope) -> None:
        print("Hello world! Received event:")
        message = json.dumps(json.loads(event.as_json()), indent=4)
        print(message)

    def close(self) -> None:
        pass
