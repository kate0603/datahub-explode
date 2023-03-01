def test1():
    from ruamel import yaml
    from big_data_public.spark.spark_context import spark
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.data_context import BaseDataContext
    from great_expectations.data_context.types.base import (
        DataContextConfig,
        InMemoryStoreBackendDefaults,
    )

    df = spark.read.json("obs://xx/dt=2022-02-14/")

    store_backend_defaults = InMemoryStoreBackendDefaults()
    data_context_config = DataContextConfig(
        store_backend_defaults=store_backend_defaults,
        checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
    )
    context = BaseDataContext(project_config=data_context_config)

    datasource_config = {
        "name": "my_spark_dataframe",
        "class_name": "Datasource",
        "execution_engine": {"class_name": "SparkDFExecutionEngine"}, # PandasExecutionEngine
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"],
            }
        },
    }

    context.test_yaml_config(yaml.dump(datasource_config))

    context.add_datasource(**datasource_config)

    # Here is a RuntimeBatchRequest using a dataframe
    batch_request = RuntimeBatchRequest(
        datasource_name="my_spark_dataframe",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="test",  # This can be anything that identifies this data_asset for you
        batch_identifiers={"batch_id": "default_identifier"},
        runtime_parameters={"batch_data": df},  # Your dataframe goes here
    )

    context.create_expectation_suite(
        expectation_suite_name="test_suite", overwrite_existing=True
    )
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )

    # all_columns = [i for i in df.columns]
    # print(all_columns)
    # suite_columns = ['app_key', 'device_id', 'role_id', 'logical_region_id', 'cp_order_id', 'cp_point']

    # ignored_columns = [i for i in all_columns if i not in suite_columns]
    # print(ignored_columns)

    # profiler = UserConfigurableProfiler(
    #    profile_dataset=validator,
    #    excluded_expectations=None,
    #    ignored_columns=ignored_columns,
    #    not_null_only=True,
    #    primary_or_compound_key=None,
    ##    semantic_types_dict=None,
    #   table_expectations_only=False,
    #   value_set_threshold="NONE",
    # )
    # suite = profiler.build_suite()
    data = validator.validate()
    # print(suite.expectation_suite_name)
    # validator
    # data = validator.get_expectation_suite(discard_failed_expectations=False)

    print(data)
    # columns = ['cp_point']
    columns = ['app_key', 'device_id', 'role_id', 'logical_region_id', 'cp_order_id', 'cp_point']
    methods = [
        # "expect_column_values_to_be_unique",
        "expect_column_values_to_not_be_null",
        # "expect_column_distinct_values_to_be_in_set",
    ]
    number_methods = [
        "expect_column_max_to_be_between",
        "expect_column_mean_to_be_between",
        "expect_column_median_to_be_between",
        "expect_column_min_to_be_between",
    ]
    columns_type = dict(df.dtypes)
    print(columns_type)
    # print(dir(validator))

    for column in columns:
        if column == 'cp_order_id':
            result = getattr(validator, 'expect_column_values_to_be_unique')(column).result
            print(column, 'expect_column_values_to_be_unique', result)
        for method in methods:
            result = getattr(validator, method)(column).result
            print(column, method, result)
        if columns_type[column] in ['bigint', 'double']:
            for method in number_methods:
                result = getattr(validator, method)(column).result
                print(column, method, result)
    # 在维度表中
    result = validator.expect_column_pair_values_to_be_in_set("logical_region_id","server_id", [(1010,1),(1012,1)]).result
    # 空值
    result = validator.expect_column_values_to_not_be_null("device_id").result
    # 值类型
    # result = validator.expect_column_values_to_be_of_type("pay_type_id", "StringType").result
    # 满足正则
    result = validator.expect_column_values_to_match_regex_list("ip", ["^(2(5[0-5]{1}|[0-4]\d{1})|[0-1]?\d{1,2})(\.(2(5[0-5]{1}|[0-4]\d{1})|[0-1]?\d{1,2})){3}$"]).result
    # 值不在列表中
    result = validator.expect_column_values_to_not_be_in_set("sp_currency_code", ['CNY']).result
    # 值在列表中
    result = validator.expect_column_values_to_be_in_set("sp_currency_code", ['CNY']).result
    # 唯一值
    result = validator.expect_column_values_to_be_unique("cp_order_id").result
    # 最大/最小/平均/标准差
    result = validator.expect_column_max_to_be_between("amount").result
    result = validator.expect_column_min_to_be_between("amount").result
    result = validator.expect_column_median_to_be_between("amount").result
    result = validator.expect_column_mean_to_be_between("amount").result # 需转为double类型
    # 转换成html
    # document_model = ValidationResultsPageRenderer(data_context=context).render(data)
    # html_code = DefaultJinjaPageView().render(document_model)
    # print(html_code)

if __name__ == '__main__':
    test1()