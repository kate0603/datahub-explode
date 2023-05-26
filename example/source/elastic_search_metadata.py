# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/28.
    Description: elasticsearch==7.13（以上版本不支持）
    Changelog: all notable changes to this file will be documented
"""
import json
from hashlib import md5
from requests import request
from datetime import date
from typing import Any, Dict, Iterable, List, Optional
from example.api.graphsql_api import DataHubQuery

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    DataPlatformInstanceClass,
    SubTypesClass,
    DatasetPropertiesClass,
    UpstreamLineageClass,
    UpstreamClass,
    DatasetLineageTypeClass,
    ChangeTypeClass,
)

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.elastic_search import (
    ElasticsearchSource,
    ElasticsearchSourceConfig,
    ElasticToSchemaFieldConverter,
)
from example.config import uat_kibana_host


class CustomElasticSearchSource(ElasticsearchSource):
    def __init__(
        self,
        host: str,
        username: str = None,
        password: str = None,
        deny_index: List = None,
    ):
        self.dt: str = date.today().strftime("%Y.%m.%d")
        deny_pattern: List = deny_index or [
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
        ]
        allow_pattern: List = [f"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*-{self.dt}"]
        ctx: PipelineContext = PipelineContext(run_id=f"test")
        config: ElasticsearchSourceConfig = ElasticsearchSourceConfig.parse_obj(
            {
                "host": host,
                "username": username,
                "password": password,
                "index_pattern": AllowDenyPattern(
                    allow=allow_pattern, deny=[f"^{i}.*" for i in deny_pattern]
                ),
            }
        )
        super(CustomElasticSearchSource, self).__init__(config, ctx)

    def create_index_pattern(self, index_pattern: str) -> None:
        base_url = uat_kibana_host
        url: str = f"{base_url}/api/saved_objects/index-pattern/{index_pattern}?overwrite=false"

        headers: dict = {
            "Content-Type": "application/json",
            "kbn-xsrf": "True",
        }
        # auth: Optional[set] = (self.username, self.password) if (
        #     self.username and self.password
        # ) else None

        index_pattern_title: str = f"{index_pattern}-*"
        data: dict = {
            "attributes": {
                "title": index_pattern_title,
                "timeFieldName": "generated_time",
            }
        }
        index_pattern_response = request(
            headers=headers, url=url, method="post", data=json.dumps(data)
        )
        index_pattern_json: dict = index_pattern_response.json()
        status: Optional[int] = index_pattern_json.get("statusCode")
        msg: str = ""
        if status and status == 409:
            msg: str = f"{index_pattern_json['error']},{index_pattern_json['message']}"
        index_pattern_id = index_pattern_json.get("id")
        if index_pattern_id:
            msg: str = f"[index-pattern: {index_pattern}]: create successfully."
        print(msg)

    def get_index_list_exist(self) -> List:
        """
        datahub上已存在的topic
        :return:
        """
        data: Dict = DataHubQuery().get_browse_list(
            data_type="DATASET", path=["prod", "elasticsearch"], start=0, count=10
        )
        count: int = data["total"]
        data: Dict = DataHubQuery().get_browse_list(
            data_type="DATASET", path=["prod", "kafka"], start=0, count=count
        )
        topic_entities: List = [i["name"] for i in data["entities"]]
        topic_groups: List = [i["name"] for i in data["groups"]]
        return topic_entities + topic_groups

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        重写，增加与datahub上的数据对比以及kibana索引模式创建
        :return:
        """
        # 新增index
        index_list: List = self.get_index_list_exist()
        indices: List = self.client.indices.get_alias()
        new_index: List = [
            i for i in indices if i.replace(f"-{self.dt}", "") not in index_list
        ]
        # todo
        new_index = ["wsy_order_receipt-2022.12.13"]
        # 新建kibana索引模式
        for index in new_index:
            self.create_index_pattern(index_pattern=index.replace(f"-{self.dt}", ""))
        # mcp
        for index in new_index:
            self.report.report_index_scanned(index)
            if self.source_config.index_pattern.allowed(index):
                for mcp in self._extract_mcps(index, is_index=True):
                    wu = MetadataWorkUnit(id=f"index-{index}", mcp=mcp)
                    self.report.report_workunit(wu)
                    yield wu
            else:
                self.report.report_dropped(index)

        for mcp in self._get_data_stream_index_count_mcps():
            wu = MetadataWorkUnit(id=f"index-{index}", mcp=mcp)
            self.report.report_workunit(wu)
            yield wu
        if self.source_config.ingest_index_templates:
            templates = self.client.indices.get_template()
            for template in templates:
                if self.source_config.index_template_pattern.allowed(template):
                    for mcp in self._extract_mcps(template, is_index=False):
                        wu = MetadataWorkUnit(id=f"template-{template}", mcp=mcp)
                        self.report.report_workunit(wu)
                        yield wu

    def _extract_mcps(
        self, index: str, is_index: bool = True
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        重写 dataset的名称,增加血缘（默认同名的kafka）
        :param index:
        :param is_index:
        :return:
        """
        dataset_index: str = index.replace(f"-{self.dt}", "")
        if is_index:
            raw_index = self.client.indices.get(index=index)
            raw_index_metadata = raw_index[index]

            # 0. Dedup data_streams.
            data_stream = raw_index_metadata.get("data_stream")
            if data_stream:
                index = data_stream
                self.data_stream_partition_count[index] += 1
                if self.data_stream_partition_count[index] > 1:
                    # This is a duplicate, skip processing it further.
                    return
        else:
            raw_index = self.client.indices.get_template(name=index)
            raw_index_metadata = raw_index[index]

        # 1. Construct and emit the schemaMetadata aspect
        # 1.1 Generate the schema fields from ES mappings.
        index_mappings = raw_index_metadata["mappings"]
        index_mappings_json_str: str = json.dumps(index_mappings)
        md5_hash = md5(index_mappings_json_str.encode()).hexdigest()
        schema_fields = list(
            ElasticToSchemaFieldConverter.get_schema_fields(index_mappings)
        )
        if not schema_fields:
            return

        # 1.2 Generate the SchemaMetadata aspect
        schema_metadata = SchemaMetadata(
            schemaName=index,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash=md5_hash,
            platformSchema=OtherSchemaClass(rawSchema=index_mappings_json_str),
            fields=schema_fields,
        )

        # 1.3 Emit the mcp
        dataset_urn: str = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_index,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
        )
        yield MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="schemaMetadata",
            aspect=schema_metadata,
            changeType=ChangeTypeClass.UPSERT,
        )

        # 2. Construct and emit the status aspect.
        yield MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="status",
            aspect=StatusClass(removed=False),
            changeType=ChangeTypeClass.UPSERT,
        )

        # 3. Construct and emit subtype
        yield MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="subTypes",
            aspect=SubTypesClass(
                typeNames=[
                    "Index Template"
                    if not is_index
                    else "Index"
                    if not data_stream
                    else "Datastream"
                ]
            ),
            changeType=ChangeTypeClass.UPSERT,
        )

        # 4. Construct and emit properties if needed. Will attempt to get the following properties
        custom_properties: Dict[str, str] = {}
        # 4.1 aliases
        index_aliases: List[str] = raw_index_metadata.get("aliases", {}).keys()
        if index_aliases:
            custom_properties["aliases"] = ",".join(index_aliases)
        # 4.2 index_patterns
        index_patterns: List[str] = raw_index_metadata.get("index_patterns", [])
        if index_patterns:
            custom_properties["index_patterns"] = ",".join(index_patterns)

        # 4.3 number_of_shards
        index_settings: Dict[str, Any] = raw_index_metadata.get("settings", {}).get(
            "index", {}
        )
        num_shards: str = index_settings.get("number_of_shards", "")
        if num_shards:
            custom_properties["num_shards"] = num_shards
        # 4.4 number_of_replicas
        num_replicas: str = index_settings.get("number_of_replicas", "")
        if num_replicas:
            custom_properties["num_replicas"] = num_replicas

        # 4.5 lifecycle
        lifecycle: str = index_settings.get("lifecycle", {}).get("name", "")
        if num_replicas:
            custom_properties["lifecycle"] = lifecycle
        # 4.6 creation_date
        creation_date: str = index_settings.get("creation_date", "")
        if num_replicas:
            custom_properties["creation_date"] = creation_date

        yield MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="datasetProperties",
            aspect=DatasetPropertiesClass(customProperties=custom_properties),
            changeType=ChangeTypeClass.UPSERT,
        )

        # 5. Construct and emit platform instance aspect
        if self.source_config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=dataset_urn,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.source_config.platform_instance
                    ),
                ),
                changeType=ChangeTypeClass.UPSERT,
            )
        # 6. 增加血缘（上游为同名的kafka）
        upstream = [["kafka", dataset_index]]
        up_class = [
            UpstreamClass(
                dataset=make_dataset_urn_with_platform_instance(
                    platform=i[0], name=i[1], platform_instance=None
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            for i in upstream
        ]

        yield MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="upstreamLineage",
            aspect=UpstreamLineageClass(upstreams=up_class),
            changeType=ChangeTypeClass.UPSERT,
        )
