# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/28.
    Description: kafka
    Changelog: all notable changes to this file will be documented
"""
import confluent_kafka
from confluent_kafka import TopicPartition
import json
from hashlib import md5
from typing import Dict, Iterable, List, Optional
from py4j.java_gateway import JavaGateway
from example.api.graphsql_api.query import DataHubQuery

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_domain_to_entity_wu
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka import (
    KafkaSourceConfig,
    KafkaSourceReport,
    KafkaSource,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    KafkaSchema,
    SchemaMetadata,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChangeTypeClass,
    DataPlatformInstanceClass,
    SubTypesClass,
    DatasetPropertiesClass
)
from datahub.utilities.registries.domain_registry import DomainRegistry


class CustomKafkaSource(KafkaSource):
    def __init__(self, bootstrap: str, deny_pattern: List = None):
        deny_pattern: List = deny_pattern or [
            "_",
            "heartbeat",
            "Old",
            "sxdpz_new_account",
            "plug_action_filter",
            "ds_",
            "ka_",
            "kaw2_",
            "kaw_",
            "lj_",
            "sdk_",
            "syx_",
            "tka_",
            "dwd",
            "error",
            "hsjx2",
            "flume",
            "gangster",
            "kailash",
            "hhdg",
            "angry",
            "all_log",
            "protocol",
            "big_data",
            "dim",
            "test",
            "-",
            r"\.",
            "rule_check",
            "consumer",
            "advertising_",
        ]
        ctx: PipelineContext = PipelineContext(run_id=f"test")
        config: KafkaSourceConfig = KafkaSourceConfig.parse_obj(
            {
                "connection": {"bootstrap": bootstrap},
                "topic_patterns": AllowDenyPattern(
                    allow=[".*"], deny=[f"^{i}.*" for i in deny_pattern]
                ),
            }
        )
        self.gateway = JavaGateway()
        self.addition_app = self.gateway.entry_point
        # 重写 去掉schema_registry_client
        super(KafkaSource, self).__init__(config, ctx)
        self.source_config: KafkaSourceConfig = config
        if (
            self.is_stateful_ingestion_configured()
            and not self.source_config.platform_instance
        ):
            raise ConfigurationError(
                "Enabling kafka stateful ingestion requires to specify a platform instance."
            )

        self.consumer: confluent_kafka.Consumer = confluent_kafka.Consumer(
            {
                "group.id": "test",
                "bootstrap.servers": self.source_config.connection.bootstrap,
                **self.source_config.connection.consumer_config,
            }
        )
        self.report: KafkaSourceReport = KafkaSourceReport()
        if self.source_config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.source_config.domain],
                graph=self.ctx.graph,
            )

    def get_topic_list_exist(self) -> List:
        """
        datahub上已存在的topic
        :return:
        """
        data: Dict = DataHubQuery().get_browse_list(
            data_type="DATASET", path=["prod", "kafka"], start=0, count=10
        )
        count: int = data["total"]
        data: Dict = DataHubQuery().get_browse_list(
            data_type="DATASET", path=["prod", "kafka"], start=0, count=count
        )
        topic_entities: List = [i["name"] for i in data["entities"]]
        topic_groups: List = [i["name"] for i in data["groups"]]
        return topic_entities + topic_groups

    def get_record_detail(self, topic) -> str:
        """
        消费数据
        :param topic:
        :return:
        """
        try:
            self.consumer.assign([TopicPartition(topic, partition=4, offset=31)])
            msg = self.consumer.poll(10)
            record = msg.value()
            self.consumer.unsubscribe()
        except Exception as e:
            print(f"get {topic} detail, error: {e}")
            record = ""
        return record

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        topics: Dict = self.consumer.list_topics().topics
        old_topic: List = self.get_topic_list_exist()
        new_topic: List = [
            i
            for i in topics.keys()
            if self.source_config.topic_patterns.allowed(i) and (i not in old_topic)
        ]
        # if len(new_topic) == 0:
        #     return []
        msg: str = f"新增topic【{','.join(new_topic)}】"
        # print(msg)
        for t in topics:
            self.report.report_topic_scanned(t)
            if self.source_config.topic_patterns.allowed(t):
                # todo
                if t != "wsy_role_transfer_server":
                    continue
                print("===", t)
                yield from self._extract_record(t)
                # add topic to checkpoint if stateful ingestion is enabled
                if self.is_stateful_ingestion_configured():
                    self._add_topic_to_checkpoint(t)
            else:
                self.report.report_dropped(t)
        if self.is_stateful_ingestion_configured():
            # Clean up stale entities.
            yield from self.gen_removed_entity_workunits()

    def field_filter(self, data: str) -> str:
        """
        json 验证
        :param data:
        :return:
        """
        try:
            data = json.loads(data)
            data = {
                key: value for key, value in data.items() if not key.startswith("@")
            }
            data = json.dumps(data)
        except Exception:
            print("json loads error")
            print(data)
        return data

    def _extract_record(self, topic: str) -> Iterable[MetadataWorkUnit]:
        # 1. Create the default dataset snapshot for the topic.
        dataset_name = topic
        platform_urn = make_data_platform_urn(self.platform)
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[Status(removed=False)],  # we append to this list later on
        )

        # 2. Attach schemaMetadata aspect (pass control to SchemaRegistry)
        # 重写schema
        record_detail = self.get_record_detail(topic=topic)
        if len(record_detail) == 0:
            fields = []
            avro_schema = ""
        else:
            try:
                record_detail = self.field_filter(record_detail)
                avro_schema = self.addition_app.jsonToAvro(record_detail, topic)
                fields = schema_util.avro_schema_to_mce_fields(avro_schema)
            except Exception as e:
                print(topic, e)
                fields = []
                avro_schema = ""

        md5_hash = md5(avro_schema.encode()).hexdigest()
        schema_metadata = SchemaMetadata(
            schemaName=topic,
            version=0,
            hash=md5_hash,
            platform=platform_urn,
            platformSchema=KafkaSchema(
                documentSchema=avro_schema,
                keySchema=avro_schema,
            ),
            fields=fields,
        )
        dataset_snapshot.aspects.append(schema_metadata)

        # 3. Attach browsePaths aspect
        browse_path_suffix = (
            f"{self.source_config.platform_instance}/{topic}"
            if self.source_config.platform_instance
            else topic
        )
        browse_path = BrowsePathsClass(
            [f"/{self.source_config.env.lower()}/{self.platform}/{browse_path_suffix}"]
        )
        dataset_snapshot.aspects.append(browse_path)

        # 4. Attach dataPlatformInstance aspect.
        if self.source_config.platform_instance:
            dataset_snapshot.aspects.append(
                DataPlatformInstanceClass(
                    platform=platform_urn,
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.source_config.platform_instance
                    ),
                )
            )
        # 5. Emit the datasetSnapshot MCE
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=f"kafka-{topic}", mce=mce)
        self.report.report_workunit(wu)
        yield wu

        # 5. Add the subtype aspect marking this as a "topic"
        subtype_wu = MetadataWorkUnit(
            id=f"{topic}-subtype",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["topic"]),
            ),
        )
        self.report.report_workunit(subtype_wu)
        yield subtype_wu

        domain_urn: Optional[str] = None

        # 6. Emit domains aspect MCPW
        for domain, pattern in self.source_config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )

        if domain_urn:
            wus = add_domain_to_entity_wu(
                entity_type="dataset", entity_urn=dataset_urn, domain_urn=domain_urn,
            )
            for wu in wus:
                self.report.report_workunit(wu)
                yield wu

