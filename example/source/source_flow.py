# -*- coding: utf-8 -*-
"""
Created on 2022/8/24
@author  :
"""
from typing import Iterable
from datahub.emitter.mce_builder import make_data_flow_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DataFlowInfoClass
from example.source import SourceBase


class SourceFlow(SourceBase):
    def __init__(self, model_obj, env: str = None):
        super().__init__(model_obj=model_obj, env=env)
        self.urn: str = make_data_flow_urn(
            orchestrator=self.model_obj.orchestrator,
            flow_id=self.model_obj.name,
            cluster=self.env,
        )

    def construct_workunits(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=self.urn,
            aspect=DataFlowInfoClass(
                name=self.model_obj.name,
                description=self.model_obj.description,
                customProperties=self.model_obj.properties,
                externalUrl=self.model_obj.external_url,
            ),
        ).as_workunit()
