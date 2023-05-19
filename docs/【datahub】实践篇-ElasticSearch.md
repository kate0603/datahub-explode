# 简介
## ElasticSearch

- [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/elasticsearch-intro.html) 是 Elastic Stack 核心的分布式搜索和分析引擎。
- Elasticsearch 为所有类型的数据提供近乎实时的搜索和分析。无论您拥有结构化或非结构化文本、数字数据还是地理空间数据，Elasticsearch 都可以以支持快速搜索的方式高效地存储和索引这些数据。
- 您可以超越简单的数据检索和聚合信息来发现数据中的趋势和模式。
- 随着数据和查询量的增长，Elasticsearch 的分布式特性使您的部署能够随之无缝增长。
## Kibana

- [Kibana](https://www.elastic.co/guide/cn/kibana/current/introduction.html) 是一款开源的数据分析和可视化平台，它是 Elastic Stack 成员之一，设计用于和 Elasticsearch 协作。
- 您可以使用 Kibana 对 Elasticsearch 索引中的数据进行搜索、查看、交互操作。您可以很方便的利用图表、表格及地图对数据进行多元化的分析和呈现。
- Kibana 可以使大数据通俗易懂。它很简单，基于浏览器的界面便于您快速创建和分享动态数据仪表板来追踪 Elasticsearch 的实时数据变化。![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1671005609804-0833c283-a093-4ac6-aa3c-f5cc2bb43e9f.png#averageHue=%23ededd1&clientId=u1dbb180b-f2ad-4&from=paste&height=729&id=ufa3fc631&originHeight=911&originWidth=2515&originalType=binary&ratio=1&rotation=0&showTitle=false&size=236825&status=done&style=none&taskId=u987b0d80-630f-4bcc-b490-99c02e6499a&title=&width=2012)
# 元数据
[示例](https://github.com/kate0603/datahub-explode/blob/main/example/source/elastic_search_metadata.py)
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1670910416763-2f2d044b-9eb6-4a16-b187-c55e9b9bfbf8.png#averageHue=%23fdfcfc&clientId=ue179cb3f-44ff-4&from=paste&height=462&id=udd708070&originHeight=578&originWidth=870&originalType=binary&ratio=1&rotation=0&showTitle=false&size=44759&status=done&style=none&taskId=uc8663833-c5ff-4f18-8a91-243468056aa&title=&width=696)
## 自定义source

- 自定义dataset_name
- 与线上datahub已有的es对比，无则告警且新增。
- 增加部分属性，如lifecyle。
- 增加上级血缘（默认是kafka同名topic）。
- 新增index的话，创建kibana索引模式。
## 常见API
### KibanaApi
```python
    def test_kibana_api(self):
        import json
        from requests import request

        index_pattern: str = "myth"
        base_url = "http://192.168.5.38:45603"
        url: str = f"{base_url}/api/saved_objects/index-pattern/{index_pattern}?overwrite=false"

        headers: dict = {
            "Content-Type": "application/json",
            "kbn-xsrf": "True",
        }
        index_pattern_title: str = "myth*"
        data = {
            "attributes": {
                "title": index_pattern_title,
                "timeFieldName": "generated_time",
            }
        }

        index_pattern_response = request(
            headers=headers, url=url, method="post", data=json.dumps(data)
        )
        index_pattern_json = index_pattern_response.json()
        if index_pattern_json['statusCode'] == 409:
            msg = f"{index_pattern_json['error']},{index_pattern_json['message']}"
        print(index_pattern_json)
```
### ElasticSearchClient
```python
    def test_es(self):
        # elasticsearch==7.13，8.0以上不兼容
        from elasticsearch import Elasticsearch
        client = Elasticsearch(
            hosts="http://pet-kibana.dianchu.cc:9200"
        )
        # 索引
        # index_list = client.indices.get_alias()
        # data = client.indices.get(index="wsy_src_chat-2022.12.06-reindex")
        # 模板
        data = client.indices.get_template()
        # 索引是否存在
        # client.indices.exists(index="")
        # 创建索引
        # client.indices.create(index="")
        # 删除索引
        # client.indices.delete(index="")
        print(data)
```
### KibanaClient
```python
    def test_kibana(self):
        from kibana_api import Kibana
        client = Kibana(base_url="http://192.168.5.38:45603/")
        objects_response = client.object().all(type="index-pattern")  # Type in specific
        objects_json = objects_response.json()
        print(objects_json)
        pattern_json = {
            "title": "myth*",
            "timeFieldName": "generated_time",  # timefiledname is important, it taken as a reference to time
            "fields": "[]"
        }
        index_pattern_response = client.object().create('index-pattern', attribs=pattern_json)
        index_pattern_json = index_pattern_response.json()
        print(index_pattern_json)
```
# 备注

- [完整项目代码](https://github.com/kate0603/datahub-explode)
- es[常用客户端工具](https://elastic-kaizen.com/)
