# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/18.
    Description:
    Changelog: all notable changes to this file will be documented
"""
from typing import Iterable
from example.config import datahub_server

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.kafka_emitter import (
    DatahubKafkaEmitter,
    KafkaEmitterConfig,
    MCE_KEY,
)
from datahub.ingestion.api.source import Source
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)


class RestSink(object):
    def __init__(self, source: Source):
        self.server: str = datahub_server["server"]
        self.token: str = datahub_server["token"]
        self.source = source

    def post(self):
        """
        :return:
        """
        wus: Iterable = self.source.get_workunits()
        emitter = DatahubRestEmitter(
            self.server, token=self.token, disable_ssl_verification=True
        )
        for wu in wus:
            if isinstance(wu.metadata, MetadataChangeEvent):
                emitter.emit_mce(wu.metadata)
            elif isinstance(
                wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            ):
                emitter.emit_mcp(wu.metadata)
            else:
                print(wu.metadata.__class__)
                print(wu.metadata)


class KafkaSink(object):
    def __init__(self, source: Source):
        self.server: str = datahub_server["server"]
        self.token: str = datahub_server["token"]
        self.source = source

    def post(self):
        """
        :return:
        """
        # todo 未测试
        import datahub.emitter.mce_builder as builder

        mce = builder.make_lineage_mce(
            [
                builder.make_dataset_urn("bigquery", "upstream1"),
                builder.make_dataset_urn("bigquery", "upstream2"),
            ],
            builder.make_dataset_urn("bigquery", "downstream"),
        )
        emitter = DatahubKafkaEmitter(
            KafkaEmitterConfig.parse_obj(
                # This is the same config format as the standard Kafka sink's YAML.
                {
                    "connection": {
                        "bootstrap": "broker:9092",
                        "producer_config": {},
                        "schema_registry_url": "http://schema-registry:8081",
                    }
                }
            )
        )

        def callback(err):
            if err:
                # Handle the metadata emission error.
                print("error:", err)

        emitter.emit(item=mce, callback=callback)
        emitter.flush()
