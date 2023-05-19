# 简介
- [Nifi介绍](https://mansui.yuque.com/gdhg3r/copv4n/tbm5lu)
- NiFil工作流
   - Nifi 流作为DataFlow实体。
   - 入口、出口处理器、远程输入和输出端口作为DataJob实体。
   - 输入和输出端口作为Dataset实体接收远程连接。
   - 通过分析出处事件，外部数据集和入口/出口处理器之间的沿袭信息。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669368194776-8762dbf5-3dc3-43c5-8b67-50793911f786.png#averageHue=%23779d9c&clientId=u573164b5-f5d1-4&from=paste&height=209&id=u97638067&originHeight=406&originWidth=942&originalType=binary&ratio=1&rotation=0&showTitle=false&size=79298&status=done&style=none&taskId=u5033d8bb-bb8b-4eec-b8f9-fb8b3a7dc59&title=&width=485)
# 元数据
[示例](https://github.com/kate0603/datahub-explode/blob/main/example/source/nifi_metadata.py)
## 自定义source
重构于datahub.ingestion.source.nifi，只上报【GenerateFlowFile】处理器。

- api创建 self.nifi_flow。
- api创建self.nifi_flow 组件的入口和出口。
- self.nifi_flow 转为mcp，上报。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669367843193-2f707106-376e-448a-8b16-7a8e3db99453.png#averageHue=%23fcfbfb&clientId=u573164b5-f5d1-4&from=paste&height=436&id=uf7748efc&originHeight=965&originWidth=678&originalType=binary&ratio=1&rotation=0&showTitle=false&size=78670&status=done&style=none&taskId=uf4fc9496-127e-4bb9-84fc-722600e1e29&title=&width=306.4000244140625)
```python
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


class CustomNifiSource(NifiSource):
    nifi.NifiProcessorProvenanceEventAnalyzer = (
        CustomNifiProcessorProvenanceEventAnalyzer
    )
```
## 血缘
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669360503058-e37a0d9a-12fa-4c3b-9414-4f20d9c3d330.png#averageHue=%23fcfcfb&clientId=u695136c6-f263-4&from=paste&height=269&id=uaacaa683&originHeight=609&originWidth=660&originalType=binary&ratio=1&rotation=0&showTitle=false&size=45223&status=done&style=none&taskId=udbdcef14-2020-4147-98e4-5f831473120&title=&width=292)
```python
    def get_lineage_mcp(self) -> MetadataChangeProposalWrapper:
        """
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
            for i in ["ods_facebook_country"]
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
```
## 运行状态

- 创建进程
- 进程关联datajob
- 开始进程
- 上报进程状态

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669360483580-a3c7300b-bf3e-4934-b667-b1de70ff746c.png#averageHue=%23fcfcfc&clientId=u695136c6-f263-4&from=paste&height=121&id=u716366ec&originHeight=348&originWidth=1478&originalType=binary&ratio=1&rotation=0&showTitle=false&size=27865&status=done&style=none&taskId=u849f9d0a-7448-48de-a248-79ff502a73e&title=&width=516)
```python
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
```
# 备注

- [完整项目代码](https://github.com/kate0603/datahub-explode)
