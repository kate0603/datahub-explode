# -*- coding: utf-8 -*-
"""
    Created by w at 2024/1/12.
    Description:
    Changelog: all notable changes to this file will be documented
"""
from example.config import datahub_server
from sqllineage.runner import LineageRunner
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)


class SQLLineage(object):
    def __init__(self):
        config = {
            "gms_server": datahub_server["server"],
            "token": datahub_server["token"],
        }
        self.env: str = "PROD"
        self.emitter = DatahubRestEmitter(**config)

    def dataset_urn(self, platform: str, table_name: str):
        """
        urn
        :param platform:
        :param table_name:
        :return:
        """
        return builder.make_dataset_urn(platform=platform, name=table_name)

    def field_urn(self, platform: str, table_name: str, field_name: str):
        """
        表、列级信息设置
        :param platform:
        :param table_name:
        :param field_name:
        :return:
        """
        return builder.make_schema_field_urn(
            parent_urn=self.dataset_urn(platform=platform, table_name=table_name),
            field_path=field_name,
        )

    def send_lineage(self, sql_path: str):
        sql_file = open(sql_path, mode="r", encoding="utf-8")
        sql: str = sql_file.read().__str__()
        # 获取sql血缘
        result = LineageRunner(sql)
        # 获取sql中的下游表名
        target_table_name: str = result.target_tables[0].__str__()
        # 获取列级血缘
        lineage = result.get_column_lineage
        # 字段级血缘list
        fine_grained_lineage_list = []
        # 用于冲突检查的上游list
        up_streams_list = []
        # 遍历列级血缘
        for column_tuples in lineage():
            # 上游list
            up_stream_str_list = []
            # 下游list
            down_stream_str_list = []
            # 逐个字段遍历
            for column in column_tuples:
                # 元组中最后一个元素为下游表名与字段名，其他元素为上游表名与字段名
                # 遍历到最后一个元素，为下游表名与字段名
                if column_tuples.index(column) == len(column_tuples) - 1:
                    down_stream_field_name = column.raw_name.__str__()
                    down_stream_table_name = (
                        column.__str__()
                        .replace("." + down_stream_field_name, "")
                        .__str__()
                    )
                    print("下游表名：" + down_stream_table_name)
                    print("下游字段名：" + down_stream_field_name)
                    down_stream_str_list.append(
                        self.field_urn(
                            platform="postgres",
                            table_name=down_stream_table_name,
                            field_name=down_stream_field_name,
                        )
                    )
                else:
                    up_stream_field_name = column.raw_name.__str__()
                    up_stream_table_name = (
                        column.__str__()
                        .replace("." + up_stream_field_name, "")
                        .__str__()
                        .replace("<default>.", "")
                    )
                    print("上游表名：" + up_stream_table_name)
                    print("上游字段名：" + up_stream_field_name)
                    up_stream_str_list.append(
                        self.field_urn(
                            platform="ods",
                            table_name=up_stream_table_name,
                            field_name=up_stream_field_name,
                        )
                    )
                    # 用于检查上游血缘是否冲突
                    up_streams_list.append(
                        Upstream(
                            dataset=self.dataset_urn(
                                platform="ods", table_name=up_stream_table_name
                            ),
                            type=DatasetLineageType.TRANSFORMED,
                        )
                    )
            fine_grained_lineage = FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.DATASET,
                upstreams=up_stream_str_list,
                downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
                downstreams=down_stream_str_list,
            )
            fine_grained_lineage_list.append(fine_grained_lineage)
        field_lineages = UpstreamLineage(
            upstreams=up_streams_list, fineGrainedLineages=fine_grained_lineage_list
        )
        lineage_mcp = MetadataChangeProposalWrapper(
            entityUrn=self.dataset_urn(
                platform="postgres", table_name=target_table_name
            ),  # 下游表名
            aspect=field_lineages,
        )
        # Emit metadata!
        self.emitter.emit_mcp(lineage_mcp)


if __name__ == "__main__":
    obj = SQLLineage()
    obj.send_lineage(sql_path="lineage_test.sql")
