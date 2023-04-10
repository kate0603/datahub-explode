# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/24.
    Description:
    Changelog: all notable changes to this file will be documented
"""
from example.custom_actions.dingtalk_action import DingTalkAction
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_user_urn,
    make_tag_urn,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.event.event_envelope import EventEnvelope
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DictWrapper,
    EntityChangeEventClass,
    MetadataChangeLogClass,
    ParametersClass,
    ChangeTypeClass,
)
from datahub_actions.event.event_registry import (
    EntityChangeEvent,
    MetadataChangeLogEvent,
)


def test_create():
    platform = "dwd"
    table = "result_facebook"
    pipeline_context = PipelineContext(pipeline_name="test", graph=None)
    metadata_change_log_event = MetadataChangeLogEvent.from_class(
        MetadataChangeLogClass(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            auditHeader=None,
            entityUrn=make_dataset_urn_with_platform_instance(
                platform=platform, name=table, platform_instance="dataPlatform"
            ),
            entityKeyAspect=None,
            aspectName="domains",
            aspect=None,
            systemMetadata=None,
            previousAspectValue=None,
            previousSystemMetadata=None,
            created=AuditStampClass(0, make_user_urn(username="datahub")),
        )
    )
    metadata_change_log_event_env = EventEnvelope(
        event_type="MetadataChangeLogEvent_v1", event=metadata_change_log_event, meta={}
    )

    entity_change_event = EntityChangeEvent.from_class(
        EntityChangeEventClass(
            entityType="dataset",
            entityUrn=make_dataset_urn_with_platform_instance(
                platform=platform, name=table, platform_instance="dataPlatform"
            ),
            category="TAG",
            operation="ADD",
            auditStamp=AuditStampClass(0, make_user_urn(username="datahub")),
            version=0,
            modifier=make_tag_urn(tag="pii"),
            parameters=ParametersClass(),
        )
    )
    entity_change_event_env = EventEnvelope(
        "EntityChangeLogEvent_v1", entity_change_event, {}
    )

    action = DingTalkAction.create({}, pipeline_context)
    action.act(metadata_change_log_event_env)
    action.act(entity_change_event_env)


if __name__ == "__main__":
    test_create()
