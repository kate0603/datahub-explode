# 简介
- Apache Superset是一个开源的、现代的、轻量级BI分析工具，能够对接多种数据源、拥有丰富的图标展示形式、支持自定义仪表盘，且拥有友好的用户界面，十分易用。
- 由于Superset能够对接常用的大数据分析工具，如Trino、Hive、Kylin、Druid等，且支持自定义仪表盘，故可作为数仓的可视化工具。
- 【datahub】收集仪表盘、图片和相关的元数据。
# web示例
## 数据集
可连接数据库（mysql等）、上传csv文件等作为数据集。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1677807277213-73d532f8-b9f9-4a48-bef5-b2e5421ff5f4.png#averageHue=%23f8f8f8&clientId=u7a97057a-e117-4&from=paste&height=446&id=uf451bd18&originHeight=558&originWidth=1893&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=49830&status=done&style=none&taskId=u1a2d3a40-8f13-4f7d-bc00-8b43442bdf4&title=&width=1514.4)

## 仪表盘
拖拽图表，搭建看板。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1677822977072-eb48e376-ed7f-48e2-b04e-1cdfb9f82c06.png#averageHue=%23f7f7f7&clientId=u7a97057a-e117-4&from=paste&height=524&id=u7287d265&originHeight=655&originWidth=1889&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=91609&status=done&style=none&taskId=udc5724f3-28a4-4edf-86a0-f4f6bd4e34b&title=&width=1511.2)
## 图表
支持柱状图、饼图、折线图等等图表。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1677813421676-bdbf16e5-d1dc-4a8d-bdc7-ae7a8aa9f5fe.png#averageHue=%23faf9f9&clientId=u7a97057a-e117-4&from=paste&height=241&id=u052c97bd&originHeight=780&originWidth=1896&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=178251&status=done&style=none&taskId=u53e3be88-6fe6-4d94-90f0-e258af7eb94&title=&width=586.4000244140625)
## SQL编辑器
自定义sql存为图表。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1677822871311-2d5b8eea-aae9-4088-9a9f-cee7c84d9a88.png#averageHue=%23f9f8f7&clientId=u7a97057a-e117-4&from=paste&height=505&id=uaab76685&originHeight=631&originWidth=1874&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=80176&status=done&style=none&taskId=u3cd8b40f-29fa-4cf1-8cc0-da6120d1fd0&title=&width=1499.2)
# datahub示例
[示例](https://github.com/kate0603/datahub-explode/blob/main/example/source/superset_metadata.py)
![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1677827370065-03616027-8b95-40f0-9d80-1b11c87c2511.png#averageHue=%23fcfcfb&clientId=u7a97057a-e117-4&from=paste&height=254&id=u074585f4&originHeight=599&originWidth=693&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=65166&status=done&style=none&taskId=uc6482dc0-919a-46c0-a9e3-b2706d0096b&title=&width=294.4000244140625)![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1677827394859-22b6e687-fbf3-4834-990a-f46f17fcbc30.png#averageHue=%23faf9f9&clientId=u7a97057a-e117-4&from=paste&height=263&id=uc2a69ff5&originHeight=525&originWidth=495&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=45365&status=done&style=none&taskId=udaf54b3d-6a8a-4250-b399-a37504e5940&title=&width=248)![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1677827423924-63c3e5a1-268b-48ad-8a4c-6f4481510b0e.png#averageHue=%23fdfdfc&clientId=u7a97057a-e117-4&from=paste&height=247&id=ubc80de22&originHeight=607&originWidth=1332&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=40925&status=done&style=none&taskId=uccb8503e-ea43-48ee-bc75-3bc84e3aadf&title=&width=541.4000244140625)
# 文档

- [datahub-source-superset](https://datahubproject.io/docs/generated/ingestion/sources/superset/)
- [superset_api](https://superset.apache.org/docs/api/)
