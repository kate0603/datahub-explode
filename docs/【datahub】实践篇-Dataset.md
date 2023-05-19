# 元数据获取途径
获取数据源的途径有多种。
## 前端UI配置

1. ingestion选择数据源

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668738153021-5a0a1ca3-0ba1-4c37-84c2-7efcd4e8962f.png#averageHue=%23b6b6b6&clientId=u3958a015-92b7-4&from=paste&height=185&id=ueac47581&originHeight=666&originWidth=2405&originalType=binary&ratio=1&rotation=0&showTitle=false&size=117686&status=done&style=none&taskId=u63ebe2e7-854d-402e-89e7-bd6f936141b&title=&width=669)
2.数据源配置以及过滤配置
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668738213793-830383af-1110-400d-8091-1ad2a48097f9.png#averageHue=%23fbfbfb&clientId=u3958a015-92b7-4&from=paste&height=381&id=u92396f0c&originHeight=947&originWidth=978&originalType=binary&ratio=1&rotation=0&showTitle=false&size=65567&status=done&style=none&taskId=u408de89a-5c67-431b-a331-4872c3b3581&title=&width=393)

3. 同步的周期

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668738589452-ffcd48a5-a2aa-40c9-84b3-16f7b78e0011.png#averageHue=%23fcfcfc&clientId=u3958a015-92b7-4&from=paste&height=252&id=u71462edc&originHeight=609&originWidth=966&originalType=binary&ratio=1&rotation=0&showTitle=false&size=53861&status=done&style=none&taskId=uf5b6d45b-cf98-447b-a920-a8403d2252a&title=&width=400)

4. 命名

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668738639925-d92e30fb-99ac-4342-9c9c-b493bf2e8d31.png#averageHue=%23fcfcfc&clientId=u3958a015-92b7-4&from=paste&height=158&id=udb97b90e&originHeight=438&originWidth=969&originalType=binary&ratio=1&rotation=0&showTitle=false&size=35326&status=done&style=none&taskId=ud32a23c8-8adc-4814-bdec-c4e738b77bb&title=&width=350)
## pipeline
postgres：配置source和sink，将数据库元数据发送datahub。
```python
from example.config import pg_db, datahub_server
from datahub.ingestion.run.pipeline import Pipeline


def run_pg():
    # The pipeline configuration is similar to the recipe YAML files provided to the CLI tool.
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "postgres",
                "config": {
                    "username": pg_db["username"],
                    "password": pg_db["password"],
                    "database": pg_db["database"],
                    "host_port": f"{pg_db['host']}:{pg_db['port']}",
                    "schema_pattern": {"allow": ["public"]},
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {
                    "server": datahub_server["server"],
                    "token": datahub_server["token"],
                },
            },
        }
    )


    # Run the pipeline and report the results.
    pipeline.run()
```
## python-emitter
```python
from typing import Iterable
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.api.source import Source
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from example.config import datahub_server


class DatahubSink(object):
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
```
# 元数据模型

- DataHub 支持基于推送和基于拉取的元数据集成。
- [示例](https://github.com/kate0603/datahub-explode/blob/main/example/source/enrich_metadata.py)
## Ingest Metadata
以python-emitter为例。
### Schema

- 字段、类型、ddl等。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669023234237-df48ad04-9367-4d58-80a9-d61da1880b94.png#averageHue=%23fdfcfc&clientId=u1920f2e5-900c-4&from=paste&height=236&id=ue8e2e1fe&originHeight=552&originWidth=823&originalType=binary&ratio=1&rotation=0&showTitle=false&size=43981&status=done&style=none&taskId=ubb804e52-af09-42c3-9fae-c18de0650d0&title=&width=351.4000244140625)![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669023246213-97e49a2e-b92e-4e1a-8ced-6e17085686e2.png#averageHue=%23f9f8f8&clientId=u1920f2e5-900c-4&from=paste&height=224&id=ue0e1451b&originHeight=423&originWidth=646&originalType=binary&ratio=1&rotation=0&showTitle=false&size=30576&status=done&style=none&taskId=u553fbc23-fc0e-4569-beab-ee129f9975f&title=&width=341.79998779296875)
```python
    def get_schema_mce(self):
        fields = [
            SchemaField(
                fieldPath="account_id",
                type=SchemaFieldDataType(type=StringTypeClass()),
                nativeDataType=repr("varchar"),
                description="账号",
                nullable=True,
                recursive=False,
            ),
            SchemaField(
                fieldPath="log_date",
                type=SchemaFieldDataType(type=DateTypeClass()),
                nativeDataType=repr("date"),
                description="日期",
                nullable=True,
                recursive=False,
            ),
        ]
        sql_ddl = """create table public.result_facebook(
                account_id varchar,
                log_date date
                );"""

        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn,
            aspects=[
                SchemaMetadata(
                    schemaName=self.table,
                    version=0,
                    fields=fields,
                    platform=make_data_platform_urn(platform=self.platform),
                    hash="",
                    platformSchema=MySqlDDL(tableSchema=sql_ddl),
                    primaryKeys=["log_date"],
                )
            ],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce
```
### Stats

- 数据资产状态

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669081691459-dc8ff84d-09b4-4603-8a49-e26f48acb4fb.png#averageHue=%23fdfdfd&clientId=uda6579f2-11ce-4&from=paste&height=170&id=ub69e9c2e&originHeight=679&originWidth=1897&originalType=binary&ratio=1&rotation=0&showTitle=false&size=67321&status=done&style=none&taskId=u77a86278-ec70-412d-a726-81d8c83f2a5&title=&width=474)
```python
    def get_status_mce(self):
        """
        :return:
        """
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn, aspects=[Status(removed=False)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce
    def get_profile_mcp(self):
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspect=DatasetProfileClass(
                timestampMillis=int(time.time()) * 1000,
                rowCount=102,
                columnCount=2,
                fieldProfiles=[
                    DatasetFieldProfileClass("account_id", max="100", min="1")
                ],
            ),
        )
        return mcp
```
### Properties

- 自定义属性。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668762750243-dd503495-44a0-4d81-ba20-17293c213acb.png#averageHue=%23fdfcfc&clientId=u436e5be9-affb-4&from=paste&height=194&id=u1b6f21b9&originHeight=443&originWidth=842&originalType=binary&ratio=1&rotation=0&showTitle=false&size=30839&status=done&style=none&taskId=ud25c9ff1-07e5-4695-bc1f-9eca02d8ca6&title=&width=369.60003662109375)
```python
    def get_properties_mce(self):
        """
        :return:
        """
        properties: Dict = {"data_location": "DWS", "database": "growth_datamine"}
        name: str = "result_facebook"
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn,
            aspects=[DatasetPropertiesClass(customProperties=properties, name=name)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce
```
### Documentation

- 可增加文字说明或者链接。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668762116346-74749e82-7681-46e2-84ab-0219ccfeff88.png#averageHue=%23fcfbfb&clientId=u436e5be9-affb-4&from=paste&height=217&id=u4c6f8949&originHeight=538&originWidth=604&originalType=binary&ratio=1&rotation=0&showTitle=false&size=33293&status=done&style=none&taskId=u1c7abf88-baab-4713-9d78-ee8d38cde3a&title=&width=243.20001220703125)
```python
    def get_description_link_mce(self):
        """
        :return:
        """
        description_link = [{"url": "https://www.baidu.com/", "description": "入口"}]
        elements_list = [
            InstitutionalMemoryMetadataClass(
                url=i["url"],
                description=i["description"],
                createStamp=AuditStampClass(
                    time=int(time.time()), actor=f"urn:li:corpuser:{self.platform}",
                ),
            )
            for i in description_link
        ]
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn, aspects=[InstitutionalMemoryClass(elements=elements_list)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce
        
	def get_editable_properties_mce(self):
        """
        :return:
        """
        description = "test_pro"
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn,
            aspects=[EditableDatasetPropertiesClass(description=description)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce
```
### Lineages

- 列血缘。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669089690248-cae2ce14-ec36-483f-a84b-9de5800c6c0e.png#averageHue=%23fdfdfd&clientId=u4d224638-a63f-4&from=paste&height=221&id=u46396417&originHeight=911&originWidth=1912&originalType=binary&ratio=1&rotation=0&showTitle=false&size=78679&status=done&style=none&taskId=u86ebecbc-e9c8-4912-a8c3-14fabbe65bb&title=&width=463)
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669089587704-64ea63dd-6b17-4091-ab1d-1f31f9b81b34.png#averageHue=%23fafafa&clientId=u4d224638-a63f-4&from=paste&height=138&id=u1fe363b2&originHeight=767&originWidth=2475&originalType=binary&ratio=1&rotation=0&showTitle=false&size=67549&status=done&style=none&taskId=u08846ce0-dfca-4734-a11f-f5f58229a99&title=&width=444)
```python
#列血缘时会验证表血缘，因此二个函数不可共存
def get_column_lineage_mcp(self):
        fine_grained_lineages: list = [
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=[
                    make_schema_field_urn(
                        make_dataset_urn(platform="ods", name="ods_facebook_country"),
                        "account_id",
                    ),
                ],
                downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
                downstreams=[
                    make_schema_field_urn(
                        self.dataset_urn,
                        "account_id",
                    ),
                ],
                transformOperation="my_func",
            )
        ]
        # 上游血缘
        upstream_lineage = [
            Upstream(
                dataset=make_dataset_urn(platform="ods", name="ods_facebook_country"),
                type=DatasetLineageType.TRANSFORMED,
            )
        ]
        field_lineages = UpstreamLineage(
            upstreams=upstream_lineage, fineGrainedLineages=fine_grained_lineages
        )

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspectName="upstreamLineage",
            aspect=field_lineages,
        )
        return mcp
        
def get_upstream_lineage_mce(self):
        """
        :return:
        """
        upstream = [["ods", "ods_facebook_country"], ["ods", "ods_facebook_adsets"]]
        up_class = [
            UpstreamClass(
                dataset=make_dataset_urn_with_platform_instance(
                    platform=i[0], name=i[1], platform_instance="dataPlatform"
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            for i in upstream
        ]
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn, aspects=[UpstreamLineageClass(upstreams=up_class)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce
```
### Queries

- 查询语句。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669080135297-47340dac-22ac-4077-b737-a8ae2285c7f6.png#averageHue=%23fbfbfb&clientId=udf3045d3-6bc0-4&from=paste&height=166&id=ue4bb2477&originHeight=360&originWidth=844&originalType=binary&ratio=1&rotation=0&showTitle=false&size=29966&status=done&style=none&taskId=u1edf2dbd-7ad0-4b4a-910c-9b08dcf3205&title=&width=390.20001220703125)
```python
    def get_queries_mcp(self):
        sql_demo = f"""select * from {self.table} limit 100"""
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspect=DatasetUsageStatisticsClass(
                timestampMillis=int(time.time() * 1000), topSqlQueries=[sql_demo]
            ),
        )
        return mcp
```
### Validation

- 质量检测。
## Enrich Metadata
在 DataHub 中注释实体的强大方法，可增强数据的可发现性并确保最终用户能够快速访问给定实体的关键上下文，例如：

- 所有权：谁负责/负责？
- 描述：预期用例是什么？存在哪些已知的警告/边缘案例？
- 词汇表术语：它与核心业务指标有什么关系？
- 域：它如何与组织域相关联？
### BrowsPath

- 显示的目录层级。
```python
    def get_brows_path_mce(self):
        """
        :return:
        """
        brows_path: list = ["/prod/dwd"]

        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn, aspects=[BrowsePathsClass(paths=brows_path)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce
```
### Glossary Terms

- 词汇表。
- 可以多个。
- 可在页面右上角管理详细信息。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668762889857-eb2a4dff-1fa3-4ad5-a781-6078f0d26daa.png#averageHue=%23fdfdfd&clientId=u436e5be9-affb-4&from=paste&height=129&id=IhKoQ&originHeight=898&originWidth=2344&originalType=binary&ratio=1&rotation=0&showTitle=false&size=120351&status=done&style=none&taskId=u16883019-b3b7-4f00-9f64-cb97d425db9&title=&width=337)
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668750680163-df844652-061f-437a-8017-eeb9798e6c01.png#averageHue=%23fdfdfd&clientId=u3958a015-92b7-4&from=paste&height=87&id=b9RgD&originHeight=427&originWidth=2492&originalType=binary&ratio=1&rotation=0&showTitle=false&size=52597&status=done&style=none&taskId=udeeb110f-69aa-449d-ac9f-21b8ee3a75f&title=&width=510)![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668751645410-a39d4a62-c756-4ba8-814e-832706888145.png#averageHue=%23fdfdfd&clientId=u3958a015-92b7-4&from=paste&height=128&id=yysyY&originHeight=451&originWidth=1917&originalType=binary&ratio=1&rotation=0&showTitle=false&size=49319&status=done&style=none&taskId=u61771fea-13a3-400b-a955-df04365e6e3&title=&width=544)

```python
      	def get_glossary_terms_mce(self):
        """
        :return:
        """
        # 需找到name对应的urn，在能关联到glossaryTerms registry
        from example.graphsql_api.query import DataHubQuery

        terms_config: list = DataHubQuery().get_root_glossary_terms()
        mapping_terms: dict = {i["properties"]["name"]: i["name"] for i in terms_config}

        terms_l = []

        for i in ["gos", "core"]:
            urn = make_term_urn(mapping_terms.get(i))
            terms_l.append(GlossaryTermAssociationClass(urn=urn))
        current_timestamp = AuditStampClass(
            time=int(time.time()), actor="urn:li:corpuser:ingestion"
        )
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn,
            aspects=[GlossaryTermsClass(terms=terms_l, auditStamp=current_timestamp)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce
```
### Global Tags

- 标签。
- 可以多个。
- 可查看标签相关的其他数据集等等

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668762862430-9ad54bd0-4209-4f43-9ce1-3997cd1a0799.png#averageHue=%23fdfdfd&clientId=u436e5be9-affb-4&from=paste&height=135&id=DfRWj&originHeight=861&originWidth=2291&originalType=binary&ratio=1&rotation=0&showTitle=false&size=118493&status=done&style=none&taskId=u81b67b44-3120-43c2-8a53-702f989535a&title=&width=360)
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668750520560-f311e9fb-e17c-4f7b-8bb9-bc430632cc36.png#averageHue=%23a0a0a0&clientId=u3958a015-92b7-4&from=paste&height=129&id=w7oMM&originHeight=596&originWidth=2349&originalType=binary&ratio=1&rotation=0&showTitle=false&size=91045&status=done&style=none&taskId=u60d939e8-78de-4c19-8a7b-90737341c99&title=&width=509)
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668750583961-b08ff4a3-23c9-45e8-a1c3-53a67c0e8116.png#averageHue=%23fcfcfb&clientId=u3958a015-92b7-4&from=paste&height=203&id=jcoHS&originHeight=565&originWidth=943&originalType=binary&ratio=1&rotation=0&showTitle=false&size=51978&status=done&style=none&taskId=u181ad950-34b3-4354-b606-ea244f57eb9&title=&width=339)
```python
    def get_global_tags_mce(self):
        """
        :return:
        """
        tags_l = []
        for i in ["facebook"]:
            tag = make_tag_urn(i)
            tags_l.append(TagAssociationClass(tag=tag))
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn, aspects=[GlobalTagsClass(tags=tags_l)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce
```
### Domain

- 领域。
- 只能有一个。
- 可在页面右上角【govern】【domains】可找到详细信息。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668762913261-4200ea4a-b3c8-4e8f-902a-1fe29cc2cd5a.png#averageHue=%23fdfdfd&clientId=u436e5be9-affb-4&from=paste&height=152&id=Fh6Jw&originHeight=840&originWidth=2279&originalType=binary&ratio=1&rotation=0&showTitle=false&size=113719&status=done&style=none&taskId=u3172ff53-b316-45b2-8545-aab5de4f4c8&title=&width=413)
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668750716996-8045ecd4-76ae-464a-af69-1dee827ec92f.png#averageHue=%23fdfdfd&clientId=u3958a015-92b7-4&from=paste&height=90&id=ClLAC&originHeight=411&originWidth=2453&originalType=binary&ratio=1&rotation=0&showTitle=false&size=49023&status=done&style=none&taskId=u1fb7c3db-217c-4485-99b7-ec1508c5a6f&title=&width=537)![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668743902822-11a7d007-4aef-44f1-abe7-cf93b9cf2973.png#averageHue=%23fbfbfa&clientId=u3958a015-92b7-4&from=paste&height=197&id=NEw8b&originHeight=362&originWidth=336&originalType=binary&ratio=1&rotation=0&showTitle=false&size=16658&status=done&style=none&taskId=uba1b8153-3330-4a2e-bc9e-84bd3e77a4b&title=&width=182.8000030517578)
```python
    def get_domain_mcp(self):
        domain: str = "发行"
        # 找到name对应的urn，才能关联到domain registry
        dh_config = DatahubClientConfig(
            server=datahub_server["server"], token=datahub_server["token"]
        )
        dhg = DataHubGraph(config=dh_config)
        domain_urn = dhg.get_domain_urn_by_name(domain_name=domain)
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspect=DomainsClass(domains=[domain_urn]),
        )
        return mcp
```
### Owner

- 拥有者。
- 可以多个。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668762828467-fbf1be0f-8bb6-4ff1-b9ab-f871348cae05.png#averageHue=%23fdfdfd&clientId=u436e5be9-affb-4&from=paste&height=113&id=xTpgG&originHeight=823&originWidth=2314&originalType=binary&ratio=1&rotation=0&showTitle=false&size=117277&status=done&style=none&taskId=uc27241ea-4546-4154-86b7-8cb47a97845&title=&width=319)
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668753755595-0005de9d-580d-4867-abbc-e8a4ce64c992.png#averageHue=%23fdfdfd&clientId=u3958a015-92b7-4&from=paste&height=198&id=zohiR&originHeight=681&originWidth=1013&originalType=binary&ratio=1&rotation=0&showTitle=false&size=45647&status=done&style=none&taskId=u3781398e-b128-4a6a-a940-0c2673202c3&title=&width=294)
```python
    def get_owner_mce(self):
        """
        :return:
        """
        owner_name = ["sj", "gos"]
        owners = [
            OwnerClass(owner=f"urn:li:corpuser:{i}", type=OwnershipTypeClass.DATAOWNER,)
            for i in owner_name
        ]
        dataset_snapshot = DatasetSnapshot(
            urn=self.dataset_urn, aspects=[OwnershipClass(owners=owners,)],
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce
```
### DatasetDeprecation

- 错误的提示。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669626635004-20049cd5-f9db-4ba0-b38a-936d7d7803b5.png#averageHue=%23f7f6f6&clientId=u6eecb489-d436-4&from=paste&height=115&id=u2d70edbd&originHeight=227&originWidth=673&originalType=binary&ratio=1&rotation=0&showTitle=false&size=43434&status=done&style=none&taskId=u7649410b-2f3d-414b-8bf1-3af7d7d2d2b&title=&width=342.4000244140625)![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669626650190-fa5bb299-aa9f-4d7c-9531-8acebf434879.png#averageHue=%23e4e2e1&clientId=u6eecb489-d436-4&from=paste&height=104&id=u584dbc25&originHeight=165&originWidth=529&originalType=binary&ratio=1&rotation=0&showTitle=false&size=26473&status=done&style=none&taskId=ub109cfbc-75ce-4039-9ee8-52693843fa1&title=&width=332.20001220703125)
```python
    def get_dataset_deprecation_mcp(self):
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=self.dataset_urn,
            aspect=DatasetDeprecationClass(
                deprecated=True,
                note="note test",
                actor=make_user_urn(username="datahub"),
                decommissionTime=int(time.time()),
            ),
        )
        return mcp
```
# GraphSQL API
前端UI提供调试页面，可直接在页面上发送请求，或者其他开发语言（python）发其api请求。
## 前端UI调试
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668673903724-41ed965b-6035-42db-8942-c583400f4985.png#averageHue=%23fbfafa&clientId=u3958a015-92b7-4&from=paste&height=279&id=u5bd04c18&originHeight=466&originWidth=523&originalType=binary&ratio=1&rotation=0&showTitle=false&size=35035&status=done&style=none&taskId=ua1dbd526-4956-412e-a59c-36c2d33c9f2&title=&width=313.3999938964844)![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668673931315-2b78d79a-cd01-446e-acb4-407d03a2708f.png#averageHue=%23f4e5b0&clientId=u3958a015-92b7-4&from=paste&height=280&id=ud63adc71&originHeight=978&originWidth=1921&originalType=binary&ratio=1&rotation=0&showTitle=false&size=149899&status=done&style=none&taskId=u750b626d-bb33-4f5d-95c2-bb517db32a7&title=&width=549)
## python-api
获取glossaryTerms列表
```python
import json
from typing import Dict, List
import requests
from example.config import datahub_server

class DataHubQuery(object):

    def request_graphql(cls, query) -> Dict:
        """
        通用请求
        :param query:
        :return:
        """
        headers = {
            "Authorization": f"Bearer {datahub_server['token']}",
            "X-RestLi-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
            "X-DataHub-Actor": "urn:li:corpuser:datahub",
        }

        res = requests.post(
            datahub_server["api"], data=json.dumps({"query": query}), headers=headers
        )
        return res.json()


    def get_root_glossary_terms(self) -> List:
        """
        glossaryTerms
        :return:[{
        "urn": "urn:li:glossaryTerm:65d36ce8-d338-4e51-8f52-eac899abce9b",
        "type": "GLOSSARY_TERM",
        "name": "65d36ce8-d338-4e51-8f52-eac899abce9b",
        "hierarchicalName": "65d36ce8-d338-4e51-8f52-eac899abce9b",
        "properties": {"name": "ncore", "__typename": "GlossaryTermProperties"},
        "__typename": "GlossaryTerm",
    },]
        """
        query: str = """query getRootGlossaryTerms {
              getRootGlossaryTerms(input: {start: 0, count: 1000}) {
                count
                start
                total
                terms {
                  ...childGlossaryTerm
                  __typename
                }
                __typename
              }
            }

            fragment childGlossaryTerm on GlossaryTerm {
              urn
              type
              name
              hierarchicalName
              properties {
                name
                __typename
              }
              __typename
            }
            """
        data: List = self.request_graphql(query)["data"]["getRootGlossaryTerms"][
            "terms"
        ]
        return data


if __name__ == "__main__":
    data = DataHubQuery().get_root_glossary_terms()
    print(data)

```

# 备注

- [完整项目代码](https://github.com/kate0603/datahub-explode)
