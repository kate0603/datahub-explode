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


class TestSource(unittest.TestCase):
    """
    """

    def setUp(self) -> None:
        pass

    # @unittest.skip("直接跳过测试")
    def test_es_metadata(self):
        source = CustomElasticSearchSource(
            host="http://pet-kibana.dianchu.cc:9200"
        )
        RestSink(source).post()

    @unittest.skip("直接跳过测试")
    def test_kafka_metadata(self):
        source = CustomKafkaSource(
            bootstrap="10.85.19.157:9092,10.85.19.158:9092,10.85.19.159:9092"
        )
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


if __name__ == "__main__":
    unittest.main()