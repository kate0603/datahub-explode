# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/28.
    Description:superset
    Changelog: all notable changes to this file will be documented
"""
import json
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.superset import (
    SupersetConfig,
    SupersetSource,
    chart_type_from_viz_type,
    get_metric_name,
    get_filter_name,
    ChartInfoClass,
    ChartSnapshot,
    ChangeAuditStamps,
    AuditStamp,
    dp,
)
from example.config import superset_url


class CustomSupersetSource(SupersetSource):
    def __init__(self):
        ctx: PipelineContext = PipelineContext(run_id=f"test")
        config: SupersetConfig = SupersetConfig.parse_obj(
            {
                "connect_uri": superset_url,
                "username": "admin",
                "password": "bigdata",
                "provider": "db",
            }
        )
        super(CustomSupersetSource, self).__init__(config=config, ctx=ctx)

    def construct_chart_from_chart_data(self, chart_data):
        chart_urn = f"urn:li:chart:({self.platform},{chart_data['id']})"
        chart_snapshot = ChartSnapshot(urn=chart_urn, aspects=[],)

        modified_actor = f"urn:li:corpuser:{(chart_data.get('changed_by') or {}).get('username', 'unknown')}"
        modified_ts = int(
            dp.parse(chart_data.get("changed_on_utc", "now")).timestamp() * 1000
        )
        title = chart_data.get("slice_name", "")

        # note: the API does not currently supply created_by usernames due to a bug, but we are required to
        # provide a created AuditStamp to comply with ChangeAuditStamp model. For now, I sub in the last
        # modified actor urn
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )
        chart_type = chart_type_from_viz_type.get(chart_data.get("viz_type", ""))
        chart_url = f"{self.config.display_uri}{chart_data.get('url', '')}"

        datasource_id = chart_data.get("datasource_id")
        datasource_urn = self.get_datasource_urn_from_id(datasource_id)

        params = json.loads(chart_data.get("params"))
        metrics = [
            get_metric_name(metric)
            for metric in (params.get("metrics", []) or [params.get("metric")])
        ]
        filters = [
            get_filter_name(filter_obj)
            for filter_obj in params.get("adhoc_filters", [])
        ]
        group_bys = params.get("groupby", []) or []
        if isinstance(group_bys, str):
            group_bys = [group_bys]
        # todo 如果是sql表达式，则是个dict，取值sqlExpression
        group_bys = [
            i["sqlExpression"] if isinstance(i, dict) else i for i in group_bys
        ]
        custom_properties = {
            "Metrics": ", ".join(metrics),
            "Filters": ", ".join(filters),
            "Dimensions": ", ".join(group_bys),
        }

        chart_info = ChartInfoClass(
            type=chart_type,
            description="",
            title=title,
            lastModified=last_modified,
            chartUrl=chart_url,
            inputs=[datasource_urn] if datasource_urn else None,
            customProperties=custom_properties,
        )
        chart_snapshot.aspects.append(chart_info)
        return chart_snapshot
