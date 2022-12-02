# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/25.
    Description:
    Changelog: all notable changes to this file will be documented
"""
import time
import uuid
from typing import Dict, Callable, Iterable, List
from datetime import datetime

from datahub.metadata.schema_classes import (
    DataProcessInstancePropertiesClass,
    AuditStampClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessRunStatusClass,
    DataProcessInstanceRunResultClass,
    DataJobInputOutputClass,
    ChangeTypeClass,
)
from datahub.configuration import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_data_job_urn_with_flow,
    make_data_flow_urn,
    make_data_process_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.ingestion.api.common import WorkUnit, PipelineContext
import datahub.ingestion.source.nifi as nifi
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.nifi import (
    NifiSourceConfig,
    NifiSource,
    NifiSourceReport,
    NifiEventType,
    ExternalDataset,
)


class CustomNifiProcessorType:
    GenerateFlowFile = "org.apache.nifi.processors.standard.GenerateFlowFile"


class CustomNifiProcessorProvenanceEventAnalyzer:
    """
    目前仅上传GenerateFlowFile这个处理器，记录调度时间，由这个处理器控制整个dag的作业状态
    """

    env: str

    KNOWN_INGRESS_EGRESS_PROCESORS = {
        CustomNifiProcessorType.GenerateFlowFile: NifiEventType.CREATE,
    }

    def __init__(self) -> None:
        # Map of Nifi processor type to the provenance event analyzer to find lineage
        self.provenance_event_to_lineage_map: Dict[
            str, Callable[[Dict], ExternalDataset]
        ] = {}


class CustomNifiConfig(ConfigModel):
    env: str = DEFAULT_ENV
    platform: str = "nifi"
    flow_id: str
    job_id: str


class CustomNifiSource(NifiSource):
    nifi.NifiProcessorProvenanceEventAnalyzer = (
        CustomNifiProcessorProvenanceEventAnalyzer
    )

    def __init__(self, flow_id: str, job_id: str, env: str = DEFAULT_ENV):
        self.config: CustomNifiConfig = CustomNifiConfig.parse_obj(
            {"flow_id": flow_id, "job_id": job_id, "env": env}
        )
        ctx: PipelineContext = PipelineContext(run_id=f"datajob_test")
        self.flow_id = self.config.flow_id
        self.platform = self.config.platform
        self.env = self.config.env
        self.job_id = self.config.job_id
        super().__init__(
            config=NifiSourceConfig(site_url="http://127.0.0.1:8443/"), ctx=ctx
        )
        self.report = NifiSourceReport()

    def get_report(self) -> NifiSourceReport:
        return self.report

    def get_workunits(self):
        self.create_nifi_flow()
        self.process_provenance_events()
        yield from self.construct_workunits()

        # self.get_workunits_lineage()
        # self.get_workunits_run()

    def get_workunits_lineage(self) -> Iterable[WorkUnit]:
        """
        血缘
        :return:
        """
        mcp = self.get_lineage_mcp()
        workunit = MetadataWorkUnit(self.job_id, mcp=mcp)
        self.report.report_workunit(workunit)
        yield workunit

    def get_workunits_run(self) -> Iterable[WorkUnit]:
        """
        进程
        :return:
        """
        # 步骤1
        mcp = self.create_run_process_mcp()
        workunit = MetadataWorkUnit(self.process_id, mcp=mcp)
        self.report.report_workunit(workunit)
        yield workunit
        # 步骤2
        mcp = self.make_run_process_relation_mcp()
        workunit = MetadataWorkUnit(self.process_id, mcp=mcp)
        self.report.report_workunit(workunit)
        yield workunit
        # 步骤3
        mcp = self.start_run_process_mcp()
        workunit = MetadataWorkUnit(self.process_id, mcp=mcp)
        self.report.report_workunit(workunit)
        yield workunit
        # 步骤4
        mcp = self.upload_run_result_mcp()
        workunit = MetadataWorkUnit(self.process_id, mcp=mcp)
        self.report.report_workunit(workunit)
        yield workunit

    def create_run_process_mcp(self):
        """
        创建process对象
        """
        self.process_id: str = str(uuid.uuid4())
        return MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            changeType="UPSERT",
            entityUrn=make_data_process_instance_urn(
                dataProcessInstanceId=self.process_id
            ),
            aspectName="dataProcessInstanceProperties",
            aspect=DataProcessInstancePropertiesClass(
                name=f"{self.job_id}-{datetime.today()}",
                type="BATCH_SCHEDULED",
                created=AuditStampClass(
                    time=int(time.time()) * 1000,
                    actor=make_user_urn(username="datahub"),
                ),
            ),
        )

    def make_run_process_relation_mcp(self):
        """
        创建进程与job的关联
        """
        return MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            changeType="UPSERT",
            entityUrn=make_data_process_instance_urn(
                dataProcessInstanceId=self.process_id
            ),
            entityKeyAspect=None,
            auditHeader=None,
            aspectName="dataProcessInstanceRelationships",
            aspect=DataProcessInstanceRelationshipsClass(
                parentTemplate=make_data_job_urn_with_flow(
                    flow_urn=make_data_flow_urn(
                        orchestrator=self.platform,
                        flow_id=self.flow_id,
                        cluster=self.env,
                    ),
                    job_id=self.job_id,
                ),
                upstreamInstances=[],
            ),
            systemMetadata=None,
        )

    def start_run_process_mcp(self):
        """
        启动进程
        """
        return MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            changeType="UPSERT",
            entityUrn=make_data_process_instance_urn(
                dataProcessInstanceId=self.process_id
            ),
            aspectName="dataProcessInstanceRunEvent",
            aspect=DataProcessInstanceRunEventClass(
                timestampMillis=int(time.time()) * 1000,
                status=DataProcessRunStatusClass.STARTED,
            ),
        )

    def upload_run_result_mcp(self, result: str = "SUCCESS"):
        """
        上传运行结果
        result的取值："SUCCESS"， "FAILURE"， "SKIPPED"
        """
        return MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            changeType="UPSERT",
            entityUrn=make_data_process_instance_urn(
                dataProcessInstanceId=self.process_id
            ),
            aspectName="dataProcessInstanceRunEvent",
            aspect=DataProcessInstanceRunEventClass(
                timestampMillis=int(time.time()) * 1000,
                status=DataProcessRunStatusClass.COMPLETE,
                result=DataProcessInstanceRunResultClass(
                    type=result, nativeResultType=self.platform
                ),
            ),
        )

    def get_lineage_mcp(self) -> MetadataChangeProposalWrapper:
        """
        血缘
        eg:
        flow_id = "1c92020f-0180-1000-cb79-26e7eb563c40"
        job_id = "f13b6633-19a9-33f4-269b-dcbbbd8d4828"
        source = CustomNifiSource(flow_id=flow_id, job_id=job_id)
        :return:
        """
        input_urn = make_data_job_urn_with_flow(
            make_data_flow_urn(
                orchestrator=self.platform, flow_id=self.flow_id, cluster=self.env
            ),
            job_id=self.job_id,
        )
        input_data_jobs: List[str] = [input_urn]
        output_data_jobs: List[str] = [
            make_dataset_urn_with_platform_instance(
                platform="ods", name=i, platform_instance=None, env=self.env,
            )
            for i in [
                "ods_facebook_country",
                "ods_facebook_adsets",
                "ods_facebook_gender_age",
            ]
        ]
        datajob_input_output = DataJobInputOutputClass(
            inputDatasets=[],
            outputDatasets=output_data_jobs,
            inputDatajobs=input_data_jobs,
        )
        mcp = MetadataChangeProposalWrapper(
            entityType="dataJob",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=input_urn,
            aspectName="dataJobInputOutput",
            aspect=datajob_input_output,
        )
        return mcp
