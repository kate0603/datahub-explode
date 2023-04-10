# -*- coding: utf-8 -*-
"""
    Created by w at 2022/12/15.
    Description:stats
    Changelog: all notable changes to this file will be documented
"""
import time
from abc import ABC
from typing import Dict, Iterable
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.api.source import SourceReport, Source, PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetProfileClass,
    DatasetFieldProfileClass,
)


class CustomProfileSource(Source, ABC):
    def __init__(
        self,
        table: str,
        rows: int,
        column_count: int,
        column_info: Dict,
        platform: str = "dwd",
    ):
        ctx: PipelineContext = PipelineContext(run_id=f"test")
        super().__init__(ctx)
        self.report = SourceReport()
        self.platform: str = platform
        self.table: str = table
        self.rows: int = rows
        self.column_count: int = column_count
        self.column_info: Dict = column_info
        self.dataset_urn: str = make_dataset_urn(
            platform=self.platform, name=self.table
        )

    def get_report(self):
        """
        :return:
        """
        return self.report

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        :return:
        """
        mcp = self.get_profile_mcp()
        workunit = MetadataWorkUnit(id="test", mcp=mcp)
        self.report.report_workunit(workunit)
        yield workunit

    def get_profile_mcp(self) -> MetadataChangeProposalWrapper:
        field_profile = [
            DatasetFieldProfileClass(
                fieldPath=i,
                max=self.column_info[i].get("max", None),
                min=self.column_info[i].get("min", None),
                # mean=self.column_info[i].get("mean", None),
                # stdev=self.column_info[i].get("stdev", None),
                uniqueCount=self.column_info[i].get("unique_count", None),
                uniqueProportion=self.column_info[i].get("unique_proportion", None),
                nullCount=self.column_info[i].get("null_count", None),
                nullProportion=self.column_info[i].get("null_proportion", None),
                sampleValues=self.column_info[i].get("sample_values", None),
            )
            for i in self.column_info
        ]
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspect=DatasetProfileClass(
                timestampMillis=int(time.time()) * 1000,
                rowCount=int(self.rows),
                columnCount=int(self.column_count),
                fieldProfiles=field_profile,
            ),
        )
        return mcp
