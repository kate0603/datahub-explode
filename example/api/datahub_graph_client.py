# -*- coding: utf-8 -*-
"""
    Created by w at 2023/5/23.
    Description:https://datahubproject.io/docs/python-sdk/clients
    Changelog: all notable changes to this file will be documented
"""
from example.config import datahub_server
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.emitter.mce_builder import make_dataset_urn
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
SchemaMetadataClass
)
from datetime import datetime, timedelta


class DatahubGraphClient(object):
    def __init__(self):
        config: DatahubClientConfig = DatahubClientConfig.parse_obj(
            {"server": datahub_server["server"], "token": datahub_server["token"]}
        )
        self.client = DataHubGraph(config=config)

    def get_domain_urn_by_name(self, domain_name: str = "运营"):
        """
        :return:
        """
        data = self.client.get_domain_urn_by_name(domain_name=domain_name)
        return data

    def get_aspects_for_entity(self, platform: str, name: str):
        """

        :param platform:
        :param name:
        :return:
        """
        urn = make_dataset_urn(platform=platform, name=name)
        data = self.client.get_aspects_for_entity(
            entity_urn=urn,
            aspects=["ownership", "domains"],
            aspect_types=[OwnershipClass, DomainsClass],
        )
        return data

    def get_usage_aspects_from_urn(self, platform: str, name: str):
        """

        :param platform:
        :param name:
        :return:
        """
        urn = make_dataset_urn(platform=platform, name=name)
        data = self.client.get_usage_aspects_from_urn(
            entity_urn=urn,
            start_timestamp=int(datetime.now().timestamp()) * 1000,
            end_timestamp=int((datetime.now() + timedelta(days=-10)).timestamp())
            * 1000,
        )
        return data

    def list_all_entity_urns(
        self, entity_type: str = "dataset", start: int = 0, count: int = 100
    ):
        """
        -  dataset : 数据集实体
        -  flow : 数据流实体
        -  dashboard : 仪表盘实体
        -  chart : 图表实体
        -  indicator : 指标实体
        -  metric : 指标实体
        -  pipeline : 数据管道实体
        -  schema : 数据模式实体
        -  tag : 标签实体
        :return:
        """
        data = self.client.list_all_entity_urns(
            entity_type=entity_type, start=start, count=count
        )
        return data

    def get_domain(self, platform: str, name: str):
        """

        :param platform:
        :param name:
        :return:
        """
        urn = make_dataset_urn(platform=platform, name=name)
        data = self.client.get_domain(entity_urn=urn)
        return data

    def get_glossary_terms(self, platform: str, name: str):
        """

        :param platform:
        :param name:
        :return:
        """
        urn = make_dataset_urn(platform=platform, name=name)
        data = self.client.get_glossary_terms(entity_urn=urn)
        return data

    def get_tags(self, platform: str, name: str):
        """

        :param platform:
        :param name:
        :return:
        """
        urn = make_dataset_urn(platform=platform, name=name)
        data = self.client.get_tags(entity_urn=urn)
        return data

    def get_domain_properties(self, platform: str, name: str):
        """

        :param platform:
        :param name:
        :return:
        """
        urn = make_dataset_urn(platform=platform, name=name)
        data = self.client.get_domain_properties(entity_urn=urn)
        return data

    def get_ownership(self, platform: str, name: str):
        """

        :param platform:
        :param name:
        :return:
        """
        urn = make_dataset_urn(platform=platform, name=name)
        data = self.client.get_ownership(entity_urn=urn)
        return data

    def get_aspect_schema(self, platform: str, name: str):
        """
        属性
        :return:
        """
        urn = make_dataset_urn(platform=platform, name=name)
        data = self.client.get_aspect_v2(
            entity_urn=urn,
            aspect_type=SchemaMetadataClass,
            aspect="schemaMetadata",
        )
        return data

    def get_config(self):
        """

        :return:
        """
        data = self.client.get_config()
        return data
