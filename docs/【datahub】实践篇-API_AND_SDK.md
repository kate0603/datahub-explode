# 简介
支持多种形式

- Python and Java SDK
- GraphQL API
- OpenAPI
- Rest.li API
# OpenAPI

- [DataHub OpenAPI Guide](https://datahubproject.io/docs/api/openapi/openapi-usage-guide/)
- [github-demo](https://github.com/kate0603/datahub-explode/blob/main/example/api/datahub_manage.py)
```python
urn = f"urn:li:dataset:(urn:li:dataPlatform:dwd,xxx,PROD)"
url = f"{server}:9002/openapi/entities/v1/?urns={urn}&soft=false"
token = f"Bearer {token}"

headers = {
    "accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": token,
}

response = requests.delete(url, headers=headers)
```
# GraphQL API

- 前端UI提供调试页面，可直接在页面上发送请求，或者其他开发语言（python）发其api请求。
- [DataHub GraphQL API](https://datahubproject.io/docs/api/graphql/overview)
## 前端UI调试
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668673903724-41ed965b-6035-42db-8942-c583400f4985.png#averageHue=%23fbfafa&clientId=u3958a015-92b7-4&from=paste&height=279&id=u5bd04c18&originHeight=466&originWidth=523&originalType=binary&ratio=1&rotation=0&showTitle=false&size=35035&status=done&style=none&taskId=ua1dbd526-4956-412e-a59c-36c2d33c9f2&title=&width=313.3999938964844)![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668673931315-2b78d79a-cd01-446e-acb4-407d03a2708f.png#averageHue=%23f4e5b0&clientId=u3958a015-92b7-4&from=paste&height=280&id=ud63adc71&originHeight=978&originWidth=1921&originalType=binary&ratio=1&rotation=0&showTitle=false&size=149899&status=done&style=none&taskId=u750b626d-bb33-4f5d-95c2-bb517db32a7&title=&width=549)
## python-api
[gitlab-demo](https://github.com/kate0603/datahub-explode/blob/main/example/api/graphsql_api/query.py)
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
## python-sdk
[github-demo](https://github.com/kate0603/datahub-explode/blob/main/example/api/datahub_graph_client.py)
# Rest.li API
[Rest.li API](https://datahubproject.io/docs/api/restli/restli-overview)
# Python Sdk

- [python sdk](https://datahubproject.io/docs/metadata-ingestion/as-a-library)
- [github-demo](https://github.com/kate0603/datahub-explode/blob/main/example/api/datahub_sdk.py)
# 文档

- [Which DataHub API is for me?](https://datahubproject.io/docs/api/datahub-apis)
- [API AND SDK](https://datahubproject.io/docs/api/tutorials/datasets/#delete-dataset)
