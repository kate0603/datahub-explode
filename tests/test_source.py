# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/29.
    Description:
    Changelog: all notable changes to this file will be documented
"""
import unittest
from example.ingestion.emitter import RestSink
from example.source.enrich_metadata import CustomEnrichSource
from example.source.nifi_metadata import CustomNifiSource
from example.source.kafka_metadata import CustomKafkaSource
from example.source.elastic_search_metadata import CustomElasticSearchSource
from example.source.csv_enricher_metadata import CustomCSVEnricherSource
from example.source.superset_metadata import CustomSupersetSource


class TestSource(unittest.TestCase):
    """
    """

    def setUp(self) -> None:
        pass

    @unittest.skip("直接跳过测试")
    def test_superset_metadata(self):
        source = CustomSupersetSource()
        RestSink(source).post()

    @unittest.skip("直接跳过测试")
    def test_csv_enricher_metadata(self):
        source = CustomCSVEnricherSource()
        RestSink(source).post()

    @unittest.skip("直接跳过测试")
    def test_es_metadata(self):
        from example.config import kibana_host

        source = CustomElasticSearchSource(host=kibana_host)
        RestSink(source).post()

    @unittest.skip("直接跳过测试")
    def test_kafka_metadata(self):
        from example.config import kafka_bootstrap

        source = CustomKafkaSource(bootstrap=kafka_bootstrap)
        # source.get_topic_list_exist()
        RestSink(source).post()

    @unittest.skip("直接跳过测试")
    def test_enrich_metadata(self):
        RestSink(source=CustomEnrichSource()).post()

    @unittest.skip("直接跳过测试")
    def test_nifi_metadata(self):
        flow_id = "1c92020f-0180-1000-cb79-26e7eb563c40"
        job_id = "f13b6633-19a9-33f4-269b-dcbbbd8d4828"
        source = CustomNifiSource(flow_id=flow_id, job_id=job_id)
        RestSink(source).post()

    @unittest.skip("直接跳过测试")
    def test_custom_dataset_profile_source(self):
        from example.source.custom_dataset_profile_source import CustomProfileSource

        source = CustomProfileSource(
            table=f"dc.dwd_active",
            rows=20,
            column_count=8,
            column_info={"data_size": {"max": "10"}},
            platform="dwd",
        )
        RestSink(source=source).post()


if __name__ == "__main__":
    unittest.main()
