# 简介
- [Kafka](https://kafka.apache.org/)是一种高吞吐量、持久性、分布式的发布订阅的消息队列系统。
- 核心能力
   - 高吞吐量：使用延迟低至 2 毫秒的机器集群以网络有限的吞吐量传递消息。
   - 可扩展：将生产集群扩展到多达 1000 个代理、每天数万亿条消息、数 PB 数据、数十万个分区。弹性扩展和收缩存储和处理。
   - 永久存储：将数据流安全地存储在分布式、持久、容错的集群中。
   - 高可用性：在可用性区域有效地扩展集群或跨地理区域连接单独的集群。![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1671007655882-53b92c6e-8a71-4370-a13e-4503b232c1d1.png#averageHue=%23f2ebc5&clientId=u8a82f08d-5fe1-4&from=paste&id=ue41593c7&originHeight=688&originWidth=1010&originalType=url&ratio=1&rotation=0&showTitle=false&size=111603&status=done&style=none&taskId=uc8b8352d-a3ee-4248-8e62-b1fe43b7734&title=)
# 元数据
[示例](https://github.com/kate0603/datahub-explode/blob/main/example/source/kafka_metadata.py)
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669863973903-8f641070-acac-467d-aa7a-752d3308947a.png#averageHue=%23fcfcfb&clientId=u1dcc0eb2-5a98-4&from=paste&height=469&id=u44a72120&originHeight=586&originWidth=932&originalType=binary&ratio=1&rotation=0&showTitle=false&size=50843&status=done&style=none&taskId=u577eccc7-c5fb-4141-af6d-cec3184a841&title=&width=745.6)
## 自定义source
增加功能：

- 与线上datahubprod-kafka列表对比，新增则告警且上报datahub。
- 去掉SchemaRegistry，改为消费数据后生成schema(需自定义容器py4jdemo，用于将json转avro)。
```python
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
```
## 其他功能
### SchemaField-v2
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669858821200-6157d75e-323b-45dd-a97b-2f727e89842a.png#averageHue=%23f9f8f7&clientId=u1dcc0eb2-5a98-4&from=paste&height=189&id=VIhHd&originHeight=236&originWidth=530&originalType=binary&ratio=1&rotation=0&showTitle=false&size=23426&status=done&style=none&taskId=u2cbfafb7-bef4-43da-8a37-7a7fecbf84a&title=&width=424)
[SchemaField-v2](https://datahubproject.io/docs/advanced/field-path-spec-v2/#the-fieldpath-encoding-schemev2) ：v2支持嵌套式的相同字段名，eg：fieldPath="[key=True].id"
### SchemaRegistry
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1669790915052-9099e5f1-5cd6-4db7-b203-ceb3f15e6943.png#averageHue=%23f8f6f5&clientId=u169916a1-3252-4&from=paste&height=206&id=u3ace129e&originHeight=649&originWidth=1107&originalType=binary&ratio=1&rotation=0&showTitle=false&size=284246&status=done&style=none&taskId=u4edbbbd3-0f53-41c7-bb87-67e2cb4e773&title=&width=352)

- [Confluent Schema Registry](http://confluent.io/docs/current/schema-registry/docs/intro.html) ：  通过在 Kafka 之上为元数据添加服务层来提供 RESTful 接口。它扩展了 Kafka，支持 Apache Avro、JSON 和 Protobuf 模式。
- 是一种服务，用于记录各种模式及其在演变过程中的不同版本。
- 30天免费试用，之后要收费。
# 备注

- [完整项目代码](https://github.com/kate0603/datahub-explode)
- 必须在3.7以及以上环境，依赖包【grpcio、networkx、google-api-python-client】
