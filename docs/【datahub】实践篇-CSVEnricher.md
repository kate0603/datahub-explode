# 简介
以csv文件的格式，覆盖或者追加 【实体/实体的列】 的其他属性，包括glossaryTerms、tags、owners、ownership_type、description、domain。
# 示例
[示例](https://github.com/kate0603/datahub-explode/blob/main/example/source/csv_enricher_metadata.py)
输入csv
![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1677736369766-e0dc9d49-c10c-441e-b981-29026199b85f.png#averageHue=%23faf8f6&clientId=u8a5aa183-36e1-4&from=paste&height=50&id=u6c784db4&originHeight=63&originWidth=1570&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=7109&status=done&style=none&taskId=u4a35da92-0eae-48ac-9594-65392b416ca&title=&width=1256)
输出结果
![image.png](https://cdn.nlark.com/yuque/0/2023/png/745518/1677736387717-486a3c8f-cbd4-42d1-9ce5-d40d0c6aed4f.png#averageHue=%23fdfdfd&clientId=u8a5aa183-36e1-4&from=paste&height=347&id=u99af86b6&originHeight=434&originWidth=1353&originalType=binary&ratio=1.25&rotation=0&showTitle=false&size=44698&status=done&style=none&taskId=ub5df2c0e-3d13-442e-a5cb-cee7ecdbd77&title=&width=1082.4)
```python
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
```
# 文档

- [datahub-source-csvenricher](https://datahubproject.io/docs/generated/ingestion/sources/csv)
