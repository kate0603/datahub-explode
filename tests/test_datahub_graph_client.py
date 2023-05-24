# -*- coding: utf-8 -*-
"""
    Created by w at 2022/5/30.
    Description: https://datahubproject.io/docs/how/delete-metadata/
    Changelog: all notable changes to this file will be documented
"""
import unittest
from example.ingestion.datahub_graph_client import DatahubGraphClient


class TestDatahubGraphClient(unittest.TestCase):
    """
    """

    def setUp(self) -> None:
        pass

    @unittest.skip("直接跳过测试")
    def test_get_domain_urn_by_name(self):
        """

        :return:urn:li:domain:25ab22d4-3217-46ee-86f8-27bb4673758b
        """
        data = DatahubGraphClient().get_domain_urn_by_name()
        print(data)

    @unittest.skip("直接跳过测试")
    def test_get_aspects_for_entity(self):
        """

        :return:{'ownership': OwnershipClass({'owners': [OwnerClass({'owner': 'urn:li:corpuser:dwd', 'type': 'DATAOWNER', 'source': None})], 'lastModified': AuditStampClass({'time': 0, 'actor': 'urn:li:corpuser:unknown', 'impersonator': None, 'message': None})}), 'domains': DomainsClass({'domains': ['urn:li:domain:25ab22d4-3217-46ee-86f8-27bb4673758b']})}
        """
        data = DatahubGraphClient().get_aspects_for_entity(
            platform="dwd", name="dc.dwd_active"
        )
        print(data)

    @unittest.skip("直接跳过测试")
    def test_list_all_entity_urns(self):
        """

        :return:
        """
        data = DatahubGraphClient().list_all_entity_urns()
        print(data)

    @unittest.skip("直接跳过测试")
    def test_get_domain(self):
        """

        :return:DomainsClass({'domains': ['urn:li:domain:25ab22d4-3217-46ee-86f8-27bb4673758b']})
        """
        data = DatahubGraphClient().get_domain(platform="dwd", name="dc.dwd_active")
        print(data)

    @unittest.skip("直接跳过测试")
    def test_get_usage_aspects_from_urn(self):
        """

        :return:
        """
        data = DatahubGraphClient().get_usage_aspects_from_urn(
            platform="dwd", name="dc.dwd_active"
        )
        print(data)

    @unittest.skip("直接跳过测试")
    def test_get_glossary_terms(self):
        """

        :return:
        """
        data = DatahubGraphClient().get_glossary_terms(
            platform="dwd", name="dc.dwd_active"
        )
        print(data)

    @unittest.skip("直接跳过测试")
    def test_get_tags(self):
        """

        :return:GlobalTagsClass({'tags': [TagAssociationClass({'tag': 'urn:li:tag:dc', 'context': None})]})
        """
        data = DatahubGraphClient().get_tags(platform="dwd", name="dc.dwd_active")
        print(data)

    @unittest.skip("直接跳过测试")
    def test_get_domain_properties(self):
        """

        :return:
        """
        data = DatahubGraphClient().get_domain_properties(
            platform="dwd", name="dc.dwd_active"
        )
        print(data)

    @unittest.skip("直接跳过测试")
    def test_get_ownership(self):
        """

        :return: OwnershipClass({'owners': [OwnerClass({'owner': 'urn:li:corpuser:dwd', 'type': 'DATAOWNER', 'source': None})], 'lastModified': AuditStampClass({'time': 0, 'actor': 'urn:li:corpuser:unknown', 'impersonator': None, 'message': None})})
        """
        data = DatahubGraphClient().get_ownership(platform="dwd", name="dc.dwd_active")
        print(data)

    @unittest.skip("直接跳过测试")
    def test_get_config(self):
        """

        :return: {'models': {}, 'patchCapable': True, 'versions': {'linkedin/datahub': {'version': 'v0.10.2', 'commit': '27c7c400022fccb7f700223c8ed95c2cb181e201'}}, 'managedIngestion': {'defaultCliVersion': '0.10.2', 'enabled': True}, 'statefulIngestionCapable': True, 'supportsImpactAnalysis': True, 'timeZone': 'GMT', 'telemetry': {'enabledCli': True, 'enabledIngestion': False}, 'datasetUrnNameCasing': False, 'retention': 'true', 'datahub': {'serverType': 'quickstart'}, 'noCode': 'true'}
        """
        data = DatahubGraphClient().get_config()
        print(data)

    @unittest.skip("直接跳过测试")
    def test_get_aspect_schema(self):
        """

        :return:
        """
        data = DatahubGraphClient().get_aspect_schema(
            platform="dwd", name="dc.dwd_active"
        )
        print(data)


if __name__ == "__main__":
    unittest.main()
