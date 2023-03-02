# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/28.
    Description:csv 覆盖或者追加domain、tag等属性
    todo csv文件中无引号
    Changelog: all notable changes to this file will be documented
"""
import os
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.csv_enricher import CSVEnricherSource, CSVEnricherConfig


class CustomCSVEnricherSource(CSVEnricherSource):
    def __init__(self):
        ctx: PipelineContext = PipelineContext(run_id=f"test")
        file_name: str = "csv_enricher_demo.csv"
        cur_path = os.path.dirname(__file__)
        file_path: str = os.path.join(cur_path, file_name)
        config: CSVEnricherConfig = CSVEnricherConfig.parse_obj(
            {"filename": file_path, "write_semantics": "OVERRIDE"}
        )
        super(CustomCSVEnricherSource, self).__init__(config=config, ctx=ctx)
