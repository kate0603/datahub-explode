# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/22.
    Description:
    Changelog: all notable changes to this file will be documented
"""
from typing import Optional, Iterable, List, Dict, Union
from example.api.datahub_sdk import DatahubSdk
from datahub.ingestion.api.source import SourceReport, Source, PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_data_flow_urn,
    make_data_job_urn_with_flow,
)


class SourceBase(Source):
    def __init__(self, model_obj, env: str = None):
        ctx: PipelineContext = PipelineContext(run_id=f"test")
        self.model_obj = model_obj
        self.env: str = env or DEFAULT_ENV
        super().__init__(ctx)
        self.report = SourceReport()
        self.urn: str = ""

    def get_report(self):
        """
        :return:
        """
        return self.report

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.construct_workunits()
        self.set_domain()
        self.set_data_product()
        self.set_data_tags()
        self.set_data_terms()
        self.set_data_owners()
        self.set_data_lineage()

    def set_data_lineage(self):
        """
        :return:
        """
        property: List = getattr(self.model_obj, "upstream_lineage", None)
        if property:
            ups: List = [
                (
                    i
                    if isinstance(i, str)
                    else make_data_job_urn_with_flow(
                        flow_urn=make_data_flow_urn(
                            orchestrator=i.flow.orchestrator,
                            flow_id=i.flow.name,
                            cluster=self.env,
                        ),
                        job_id=i.name,
                    )
                )
                for i in property
            ]
            print(ups)
            DatahubSdk().update_lineage(down_urn=self.urn, up_urns=ups)

    def set_data_owners(self):
        property: List = getattr(self.model_obj, "data_owner", None)
        if property:
            DatahubSdk().add_owners_list(
                urn=self.urn,
                owners=property,
            )

    def set_data_terms(self):
        property: List = getattr(self.model_obj, "glossary_terms", None)
        if property:
            DatahubSdk().add_terms_list(
                urn=self.urn,
                terms=property,
            )

    def set_data_tags(self):
        property: List = getattr(self.model_obj, "global_tags", None)
        if property:
            DatahubSdk().add_tags_list(
                urn=self.urn,
                tags=property,
            )

    def set_domain(self):
        property: str = getattr(self.model_obj, "domain", None)
        if property:
            DatahubSdk().add_domain(
                urn=self.urn,
                domain=property,
            )

    def set_data_product(self):
        property: str = getattr(self.model_obj, "data_product", None)
        if property:
            DatahubSdk().batch_set_data_product(
                dataset_list=[self.urn],
                name=property,
            )

    def construct_workunits(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        pass
