# -*- coding: utf-8 -*-
"""
    Created by wq at 2023/3/1.
    Copyright (c) 2013-present, Xiamen Dianchu Technology Co.,Ltd.
    Description:
    Changelog: all notable changes to this file will be documented
"""
from ruamel import yaml
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.data_context import BaseDataContext
from great_expectations.core.batch import RuntimeBatchRequest


CONNECTION_STRING = "postgresql+psycopg2://user:password@ip:port/database"

store_backend_defaults = InMemoryStoreBackendDefaults()
data_context_config = DataContextConfig(
    store_backend_defaults=store_backend_defaults,
    checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
)
context = BaseDataContext(project_config=data_context_config)

datasource_config = {
    "name": "my_postgres_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "include_schema_name": True,
        },
    },
}

datasource_config["execution_engine"]["connection_string"] = CONNECTION_STRING
context.test_yaml_config(yaml.dump(datasource_config))
context.add_datasource(**datasource_config)

# Here is a RuntimeBatchRequest using a query
batch_request = RuntimeBatchRequest(
    datasource_name="my_postgres_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data
    runtime_parameters={"query": "SELECT * from public.dim_game_info_1 LIMIT 10"},
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())
