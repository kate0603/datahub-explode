# -*- coding: utf-8 -*-
"""
Created on 2022/8/24
@author  :
"""
from typing import Optional, Iterable, List, Dict, Union
from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
)
from example.source import SourceBase


class SourceJob(SourceBase):
    def __init__(self, model_obj, env: str = None):
        super().__init__(model_obj=model_obj, env=env)
        flow_urn = make_data_flow_urn(
            orchestrator=self.model_obj.flow.orchestrator,
            flow_id=self.model_obj.flow.name,
            cluster=self.env,
        )
        self.urn: str = make_data_job_urn_with_flow(
            flow_urn=flow_urn, job_id=self.model_obj.name
        )

    def construct_workunits(self) -> Iterable[MetadataWorkUnit]:
        external_url: str = self.model_obj.external_url
        name: str = self.model_obj.name
        job_type: str = self.model_obj.job_type
        description: str = self.model_obj.description
        job_properties: Dict = self.model_obj.properties
        status: str = self.model_obj.status

        if job_properties:
            job_properties = {k: v for k, v in job_properties.items() if v is not None}

        yield MetadataChangeProposalWrapper(
            entityUrn=self.urn,
            aspect=DataJobInfoClass(
                name=name,
                type=job_type,
                description=description,
                customProperties=job_properties,
                externalUrl=external_url,
                status=status,
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=self.urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=[], outputDatasets=[], inputDatajobs=[]
            ),
        ).as_workunit()
