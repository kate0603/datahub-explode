# 背景	
## 企业数据难点
企业数据面临的难点包括以下几个方面： 

1.  数据来源：企业数据来自于各个不同的来源，并且它们之间可能存在格式和结构的差异，这给数据整合和分析带来了难度。 
2.  数据质量：企业数据存在许多质量问题，包括重复、缺失、不一致等，这些问题可能导致分析结果不准确，从而影响业务决策。 
3. 数据安全性：随着数据泄露和黑客攻击事件的增多，企业数据的安全性和隐私保护变得更加重要。企业需要采取相应的措施来确保数据的安全和合规性。 
4. 数据管理：企业可能拥有大量的数据，而这些数据分布在不同的地方，可能难以管理。需要实现数据集中存储、标准化和分类，以实现更好的数据管理。 
5. 数据分析：分析大量的数据需要使用复杂的数据分析工具和技术，需要有专业的数据分析人才和团队来完成。同时，数据分析可能还受到隐私和安全等问题的限制。 
## 元数据

- 元数据是描述数据的数据，其目的是为了帮助理解和管理数据本身。它包括数据的属性、结构、定义以及数据如何被使用、管理和组织等方面的信息。
- 元数据可以帮助我们更好地理解数据，包括数据的来源、数据质量、数据的用途和数据的含义等信息，同时也提供了数据分类、整理和搜索的支持。
- 元数据在数据管理、数据治理、数据分析等方面都有非常重要的作用，可以帮助我们提高数据的质量和价值，从而更好地支持业务需求。
- 理解元数据，我们可以简单从三个字母开始：
   - A（Application Context）：关于数据的描述（创建者、创建时间、标签、字段等等）；
   - B（Behavior）：关于数据如何被使用或消费（何时、被何任务、被谁、结果如何等等）；
   - C（Change）：关于数据的变化（数据是否出现了增删改查的变化等等）；
## 元数据管理平台

- 元数据管理平台提供了一个对企业数据资产进行维护、管理和文档化的标准化方法。这是因为当企业的数据资产逐渐增长时，管理数据资产变得越来越复杂和困难。
- 以下是为什么需要元数据管理平台的一些原因： 
   1. 数据的复杂性：企业中的数据很可能非常复杂，由多种不同类型的数据、格式和结构组成。元数据管理平台可以提供对数据的详细描述和标记，帮助人们更好地理解数据的组成和使用方式。 
   2. 数据的安全性：元数据管理平台可以帮助确定数据的敏感性和安全需求，并为数据提供相应的保护措施。 
   3.  数据的可重用性：元数据管理平台可以提供有关数据的详细描述信息，包括数据的来源、格式、结构和用途，这些信息可以帮助人们更好地重用数据，从而减少重复开发。 
   4. 数据的可发现性：元数据管理平台可以帮助人们更好地搜索、浏览和发现数据，从而更好地管理和使用数据。 
   5. 数据的文档化：元数据管理平台可以帮助人们记录数据的详细信息，包括数据的定义、来源、格式、结构、用途等，从而提高数据的可理解性和可维护性。 

综合以上点，元数据管理平台的好处包括提高数据质量、降低风险、提高数据可重用性、提高数据工作效率等。
# 简介

- [DataHub](https://datahubproject.io/docs/) 是由LinkedIn的数据团队开源的一款提供元数据搜索与发现的工具。
- DataHub  是第三代元数据平台，支持为现代数据堆栈构建的数据发现、协作、治理和端到端可观察性。
- 采用模型优先的理念，专注于解锁不同工具和系统之间的互操作性。
- 这个可扩展的元数据平台是为开发人员构建的，用于驯服其快速发展的数据生态系统的复杂性，以及为数据从业者构建的，以利用其组织内数据的总价值。
- DataHub 帮助您发现和组织组织内的重要数据：
   - 快速搜索数据集、仪表板、数据管道等。
   - 查看并了解有关如何创建、转换和使用数据的完整端到端沿袭。
   - 深入了解组织内的其他人如何使用数据。
   - 定义所有权。
## 特点
[功能清单](https://github.com/datahub-project/datahub/blob/master/docs/features.md)

- 搜索和发现：搜索体验显示跨平台，快速了解数据端对端的旅程，主动识别哪些实体可能受到重大变更的影响，结合技术和逻辑元数据以提供数据实体的 360º 视图。

![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-search-all-corners-of-your-datastack.gif#from=url&id=jDr5S&originHeight=554&originWidth=800&originalType=binary&ratio=1&rotation=0&showTitle=false&status=done&style=none&title=)

- 数据治理：实时治理，管理数据（标签，词汇表术语，领域，血缘等）。

![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-tags-terms-domains.png#from=url&id=icoOn&originHeight=2166&originWidth=2894&originalType=binary&ratio=1&rotation=0&showTitle=false&status=done&style=none&title=)

- 数据中心管理：用户、组等访问策略。

![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-manage-policies.png#from=url&id=Prvw1&originHeight=2303&originWidth=3179&originalType=binary&ratio=1&rotation=0&showTitle=false&status=done&style=none&title=)
## 架构
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668588324423-d656e1a7-54fb-418a-80f0-713882ede106.png#averageHue=%23f9f7f5&clientId=ub7705cf5-8cdd-4&from=paste&id=u3a3513f3&originHeight=737&originWidth=1320&originalType=url&ratio=1&rotation=0&showTitle=false&size=354968&status=done&style=none&taskId=u3b9d10cf-476f-4bcf-8ab1-96e7e2c3d7f&title=)	 	 	

- 前端：Ember + TypeScript + ES9 + ES.Next + Yarn + ESLint
- 服务端：Play Framework（web框架） + Spring + Rest.li（restful框架）+ Pegasus（数据建模语言） + Apache Samza （流处理框架）
- 基础设施：elastic search(5.6) + Mysql + neo4j + kafka
- 构建工具：Gradlew + Docker + Docker compose
# 应用场景
举例：

- 角色登录在哪里？（模糊搜索）

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1671010310459-6c175b74-314e-4071-896d-791d83475a07.png#averageHue=%23fcfbfb&clientId=uc1c0af92-fbcd-4&from=paste&height=206&id=m497K&originHeight=396&originWidth=696&originalType=binary&ratio=1&rotation=0&showTitle=false&size=35652&status=done&style=none&taskId=u8d7a3d12-5e18-4fb9-a8ba-d01fc680646&title=&width=362.79998779296875)

- 登录表有哪些字段？（schema）

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1671010330899-71fbccb5-41aa-4eed-ba72-4031881b0bb9.png#averageHue=%23fdfcfc&clientId=uc1c0af92-fbcd-4&from=paste&height=275&id=SiS1Y&originHeight=552&originWidth=817&originalType=binary&ratio=1&rotation=0&showTitle=false&size=44400&status=done&style=none&taskId=ubc884dc4-b63c-46c6-8063-a22de896893&title=&width=406.60003662109375)

- 文档太多，忘记文档路径？（document）

![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1683687660144-12714ce5-5677-42d5-90c2-afa5493de603.png#averageHue=%23fcfcfb&clientId=u1ae40b6b-bfe0-4&from=paste&height=331&id=u0d0877a9&originHeight=540&originWidth=413&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=31951&status=done&style=none&taskId=ud7d5e585-ef22-4336-b9d8-8216e282e7c&title=&width=253.39999389648438)

- 表中的字段上游是哪里？下游是哪里？（lineages）

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1671010394792-c922b278-b881-4ce4-9fab-a475499b9f53.png#averageHue=%23fcfbfb&clientId=uc1c0af92-fbcd-4&from=paste&height=294&id=iLodd&originHeight=576&originWidth=667&originalType=binary&ratio=1&rotation=0&showTitle=false&size=49707&status=done&style=none&taskId=ua76ca570-7ff7-4141-aa45-5f6a49f867f&title=&width=340.60003662109375)
# 组成
![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668648313654-6a5c99d6-c49b-4157-a228-aae0b7c6b1f3.png#averageHue=%23afafae&clientId=ueef69fd6-3c27-4&from=paste&id=kFZbb&originHeight=977&originWidth=520&originalType=url&ratio=1&rotation=0&showTitle=false&size=94255&status=done&style=none&taskId=u1ec028c7-20d4-4e84-a980-85142e77407&title=)
## Metadata Store
负责存储构成metadata graph的entities和aspects。它由托管一组Rest.li API 端点的 Spring Java 服务以及用于主存储和索引的 MySQL、Elasticsearch 和 Kafka 组成。
## Metadata Model
[元数据模型](https://datahubproject.io/docs/metadata-modeling/metadata-model)是定义构成metadata graph的entities和aspects的形状以及它们之间关系的模式

- Entities：实体是元数据图中的主节点，由类型（eg:dataset）、唯一标识(eg:urn)和属性组(aspects， eg：documents)构成。
   - 常见实体：[dataset](https://datahubproject.io/docs/generated/metamodel/entities/dataset/)、[Data Job (Task)](https://datahubproject.io/docs/generated/metamodel/entities/datajob/)、[Data Flow (Pipeline)](https://datahubproject.io/docs/generated/metamodel/entities/dataflow/)
- Aspects：描述实体的特定方面的属性集合。
   - 常见aspects：ownership、[globalTags](https://datahubproject.io/docs/generated/metamodel/entities/tag)、[glossaryTerms](https://datahubproject.io/docs/generated/metamodel/entities/glossaryterm)、[domain](https://datahubproject.io/docs/generated/metamodel/entities/domain)、status。
## Ingestion Framework
[元数据提取框架](https://datahubproject.io/docs/metadata-ingestion/)：是一个模块化的、可扩展的 Python 库，用于从外部源系统（例如 Snowflake、Looker、MySQL、Kafka）中提取元数据，将其转换为 DataHub 的元数据模型，并通过 Kafka 或使用元数据存储 Rest API 将其写入 DataHub。![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668649170575-4883e796-83c1-47e5-b447-b0a6a4a28371.png#averageHue=%23fcf9f7&clientId=ueef69fd6-3c27-4&from=paste&id=ud655d44b&originHeight=1798&originWidth=2838&originalType=url&ratio=1&rotation=0&showTitle=false&size=1223828&status=done&style=none&taskId=uf2a0b980-8893-42d9-bb02-ba5d03a4e31&title=)
### Metadata Ingestion
核心概念

- Sources：从中提取元数据的数据，包括：spark、great expectations、bigquery、clickhouse、csv、hive、kafka、mysql、postgres、tableau
- Sinks：元数据的目的地，通过REST（datahub-sink）或Kafka（datahub-kafka） sink 将元数据发送到 DataHub。
- Recipes：它告诉我们的摄取脚本从哪里（source）提取数据以及将它放在哪里（sink）。
- [Transformers](https://datahubproject.io/docs/metadata-ingestion/docs/transformer/intro)：元数据到达摄取接收器之前对其进行修改——例如，我们可能希望添加自定义标签、所有权、属性或修补某些字段。包括：ownership、status、globalTags、glossaryTerms、domin、lineages等
### Metadata Service (Also known as GMS)
[元数据服务](https://datahubproject.io/docs/metadata-service)

- 一个公共[ GraphQL](https://graphql.org/) API，用于获取和改变元数据图上的对象。
- 一个通用的 [Rest.li](https://linkedin.github.io/rest.li/) API，用于摄取构成元数据图的底层存储模型。
### Metadata Events
[元数据事件](https://datahubproject.io/docs/what/mxe)，使用一些重要的 Kafka 事件进行操作：

-  Metadata Change Event (MCE)：Kafka Streams作业，监听 DataHub 客户端发出的请求更改metadata graph的更改提议事件，然后它将这些请求应用于 DataHub 的存储层：元数据服务。
- Metadata Audit Event (MAE)：Kafka Streams作业，监听因对metadata graph所做的更改而发出的更改日志事件，将元数据模型中的更改转换为针对二级搜索和图索引的更新。
- Metadata Change Proposal (MCP)：为给定的aspects提供一个新的值，将请求的更改应用到metadata graph。（比mce和mae更加通用）
- Metadata Change Log (MCL)：对metadata graph所做的任何更改。将更改写入持久存储后，元数据更改日志事件会立即发送到 Kafka。（比mce和mae更加通用）

![image.png](https://cdn.nlark.com/yuque/0/2022/png/745518/1668649489983-984e4b00-f0c8-4070-ba6e-ed1edffdcd34.png#averageHue=%23f5ece1&clientId=ueef69fd6-3c27-4&from=paste&id=udf7c99c2&originHeight=1658&originWidth=2936&originalType=url&ratio=1&rotation=0&showTitle=false&size=1685323&status=done&style=none&taskId=u65c03c52-49a7-4620-bb60-1c3cab3f48b&title=)
## GraphQL API
提供了一个强类型的、面向实体的 [API](https://datahubproject.io/docs/api/graphql/overview)，它使得与构成元数据图的实体的交互变得简单。
## User Interface
DataHub 带有 React UI，包括一组不断发展的功能，使发现、管理和调试您的数据资产变得轻松愉快。
# 文档

- [官方文档](https://datahubproject.io/docs/)
- [gitlab代码仓库](https://github.com/datahub-project/datahub)
- [数据治理之数据发现-元数据-DataHub](https://blog.csdn.net/ZYC88888/article/details/119213446)、[元数据，让数据不卡脖子 - 数据发现（搜索）平台演进与产品清单（含开源）](https://coffee.pmcaff.com/article/2819291299741824/pmcaff?utm_source=forum)、[一站式元数据治理平台——Datahub入门宝典](https://mp.weixin.qq.com/s?__biz=Mzg3MTIzOTgxOA==&mid=2247486661&idx=1&sn=64c47554d93f8fce8d6242ebd2ecde14&chksm=ce80d520f9f75c36707a05e04e4b6042903ad45ea0e515d2c09ec6bc948bd293c3a6f4163b5f&token=326072703&lang=zh_CN&scene=21#wechat_redirect)



