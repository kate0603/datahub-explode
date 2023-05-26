# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/18.
    Description:tag domain glossary_terms 等等
    Changelog: all notable changes to this file will be documented
"""

import time
from abc import ABC
from typing import Iterable, Dict
from example.config import datahub_server

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mce_builder import (
    make_term_urn,
    make_tag_urn,
    make_dataset_urn,
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_user_urn,
)
from datahub.ingestion.api.source import SourceReport, Source, PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    DateTypeClass,
    MySqlDDL,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    DomainsClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    DatasetPropertiesClass,
    UpstreamLineageClass,
    UpstreamClass,
    DatasetLineageTypeClass,
    EditableDatasetPropertiesClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    AuditStampClass,
    ChangeTypeClass,
    DatasetUsageStatisticsClass,
    DatasetProfileClass,
    DatasetFieldProfileClass,
    TestResultsClass,
    TestResultClass,
    TestResultTypeClass,
    TestInfoClass,
    TestDefinitionClass,
    TestDefinitionTypeClass,
    DatasetDeprecationClass,
    BrowsePathsClass,
    QuantileClass,
    ValueFrequencyClass,
    HistogramClass,
)
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig


class CustomEnrichSource(Source, ABC):
    def __init__(self):
        ctx: PipelineContext = PipelineContext(run_id=f"test")
        super().__init__(ctx)
        self.report = SourceReport()
        self.platform = "dwd"
        self.table = "public.result_facebook"
        self.dataset_urn = make_dataset_urn(platform=self.platform, name=self.table)

    def get_report(self):
        """
        :return:
        """
        return self.report

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        :return:
        """
        for mcp in [
            # self.get_dataset_deprecation_mcp()
            # self.get_column_lineage_mcp()
            # self.get_test_result_mcp(),
            self.get_profile_mcp(),
            # self.get_domain_mcp(),
            # self.get_queries_mcp(),
        ]:
            workunit = MetadataWorkUnit(id="test", mcp=mcp)
            self.report.report_workunit(workunit)
            yield workunit
        for mce in [
            # self.get_status_mce(),
            # self.get_schema_mce(),
            # self.get_properties_mce(),
            # self.get_description_link_mce(),
            # self.get_editable_properties_mce(),
            # self.get_upstream_lineage_mce(),
            # self.get_owner_mce(),
            # self.get_glossary_terms_mce(),
            # self.get_global_tags_mce(),
        ]:
            workunit = MetadataWorkUnit(id="test", mce=mce)
            self.report.report_workunit(workunit)
            yield workunit

    def get_schema_mce(self):
        fields = [
            SchemaField(
                fieldPath="account_id",
                type=SchemaFieldDataType(type=StringTypeClass()),
                nativeDataType=repr("varchar"),
                description="账号",
                nullable=True,
                recursive=False,
            ),
            SchemaField(
                fieldPath="log_date",
                type=SchemaFieldDataType(type=DateTypeClass()),
                nativeDataType=repr("date"),
                description="日期",
                nullable=True,
                recursive=False,
            ),
        ]
        sql_ddl = """create table public.result_facebook(
                account_id varchar,
                log_date date
                );"""

        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn,
            aspects=[
                SchemaMetadata(
                    schemaName=self.table,
                    version=0,
                    fields=fields,
                    platform=make_data_platform_urn(platform=self.platform),
                    hash="",
                    platformSchema=MySqlDDL(tableSchema=sql_ddl),
                    primaryKeys=["log_date"],
                )
            ],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce

    def get_status_mce(self):
        """
        :return:
        """
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn, aspects=[Status(removed=False)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce

    def get_properties_mce(self):
        """
        :return:
        """
        properties: Dict = {"data_location": "DWS", "database": "growth_datamine"}
        name: str = "result_facebook"

        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn,
            aspects=[DatasetPropertiesClass(customProperties=properties, name=name)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce

    def get_brows_path_mce(self):
        """
        :return:
        """
        brows_path: list = ["/prod/dwd"]

        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn, aspects=[BrowsePathsClass(paths=brows_path)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce

    def get_description_link_mce(self):
        """
        :return:
        """
        description_link = [{"url": "https://www.baidu.com/", "description": "入口"}]
        elements_list = [
            InstitutionalMemoryMetadataClass(
                url=i["url"],
                description=i["description"],
                createStamp=AuditStampClass(
                    time=int(time.time()), actor=make_user_urn(username=self.platform),
                ),
            )
            for i in description_link
        ]
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn,
            aspects=[InstitutionalMemoryClass(elements=elements_list)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce

    def get_editable_properties_mce(self):
        """
        :return:
        """
        description = "test_pro"
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn,
            aspects=[EditableDatasetPropertiesClass(description=description)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce

    def get_upstream_lineage_mce(self):
        """
        :return:
        """
        upstream = [["ods", "ods_facebook_country"], ["ods", "ods_facebook_adsets"]]
        up_class = [
            UpstreamClass(
                dataset=make_dataset_urn_with_platform_instance(
                    platform=i[0], name=i[1], platform_instance=None
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            for i in upstream
        ]
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn, aspects=[UpstreamLineageClass(upstreams=up_class)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce

    def get_column_lineage_mcp(self):
        fine_grained_lineages: list = [
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=[
                    make_schema_field_urn(
                        make_dataset_urn(platform="ods", name="ods_facebook_country"),
                        "account_id",
                    ),
                ],
                downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
                downstreams=[make_schema_field_urn(self.dataset_urn, "account_id",),],
                transformOperation="my_func",
            )
        ]
        # 上游血缘
        upstream_lineage = [
            Upstream(
                dataset=make_dataset_urn(platform="ods", name="ods_facebook_country"),
                type=DatasetLineageType.TRANSFORMED,
            )
        ]
        field_lineages = UpstreamLineage(
            upstreams=upstream_lineage, fineGrainedLineages=fine_grained_lineages
        )

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspectName="upstreamLineage",
            aspect=field_lineages,
        )
        return mcp

    def get_owner_mce(self):
        """
        :return:
        """
        owner_name = ["sj", "gos"]
        owners = [
            OwnerClass(
                owner=make_user_urn(username=i), type=OwnershipTypeClass.DATAOWNER,
            )
            for i in owner_name
        ]
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn, aspects=[OwnershipClass(owners=owners,)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce

    def get_glossary_terms_mce(self):
        """
        :return:
        """
        # 需找到name对应的urn，在能关联到glossaryTerms registry
        from example.api.graphsql_api import DataHubQuery

        terms_config: list = DataHubQuery().get_root_glossary_terms()
        mapping_terms: dict = {i["properties"]["name"]: i["name"] for i in terms_config}

        terms_l = []

        for i in ["gos", "core"]:
            urn = make_term_urn(mapping_terms.get(i))
            terms_l.append(GlossaryTermAssociationClass(urn=urn))
        current_timestamp = AuditStampClass(
            time=int(time.time()), actor=make_user_urn(username="ingestion")
        )

        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn,
            aspects=[GlossaryTermsClass(terms=terms_l, auditStamp=current_timestamp)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce

    def get_global_tags_mce(self):
        """
        :return:
        """
        tags_l = []
        for i in ["facebook"]:
            tag = make_tag_urn(i)
            tags_l.append(TagAssociationClass(tag=tag))
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn, aspects=[GlobalTagsClass(tags=tags_l)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce

    def get_domain_mcp(self):
        domain: str = "发行"
        # 找到name对应的urn，才能关联到domain registry
        dh_config = DatahubClientConfig(
            server=datahub_server["server"], token=datahub_server["token"]
        )
        dhg = DataHubGraph(config=dh_config)
        domain_urn = dhg.get_domain_urn_by_name(domain_name=domain)
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspect=DomainsClass(domains=[domain_urn]),
        )
        return mcp

    def get_queries_mcp(self):
        sql_demo = f"""select * from {self.table} limit 100"""
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspect=DatasetUsageStatisticsClass(
                timestampMillis=int(time.time() * 1000), topSqlQueries=[sql_demo]
            ),
        )
        return mcp

    def get_profile_mcp(self):
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspect=DatasetProfileClass(
                timestampMillis=int(time.time()) * 1000,
                rowCount=102,
                columnCount=2,
                fieldProfiles=[
                    DatasetFieldProfileClass(
                        fieldPath="account_id",
                        max="10",
                        min="1",
                        mean="50",
                        median="49",
                        stdev="2.3",
                        uniqueCount=5,
                        uniqueProportion=5.1,
                        nullCount=8,
                        nullProportion=8.1,
                        quantiles=[QuantileClass(quantile="2", value="2")],
                        distinctValueFrequencies=[
                            ValueFrequencyClass(value="a", frequency=3)
                        ],
                        histogram=HistogramClass(boundaries=["a", "b"], heights=[1, 2]),
                        sampleValues=["a"],
                    )
                ],
            ),
        )
        return mcp

    def get_dataset_deprecation_mcp(self):
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspect=DatasetDeprecationClass(
                deprecated=True,
                note="note test",
                actor=make_user_urn(username="datahub"),
                decommissionTime=int(time.time()),
            ),
        )
        return mcp

    def get_test_result_mcp(self):
        # todo 待完善
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspect=TestResultsClass(
                failing=[
                    TestResultClass(
                        test=TestInfoClass(
                            name="Dataset has domain",
                            category="Metadata Completeness",
                            definition=TestDefinitionClass(
                                type=TestDefinitionTypeClass().JSON, json=""
                            ),
                            description="test_des",
                        ).__str__(),
                        type=TestResultTypeClass().FAILURE,
                    )
                ],
                passing=[],
            ),
        )
        return mcp
