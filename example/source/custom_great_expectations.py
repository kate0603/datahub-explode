# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/22.
    Description:
    Changelog: all notable changes to this file will be documented
"""
# from example.config import datahub_server
from example.source.datahub_validation import DataHubValidation

# from datahub.integrations.great_expectations.action import DataHubValidationAction
import pandas as pd
from ruamel import yaml
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.data_context import BaseDataContext
from great_expectations.core.batch import RuntimeBatchRequest


class CustomGreatExpectations(object):
    def run(self):
        store_backend_defaults = InMemoryStoreBackendDefaults()
        data_context_config = DataContextConfig(
            store_backend_defaults=store_backend_defaults,
            checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
        )
        context = BaseDataContext(project_config=data_context_config)
        context._usage_statistics_handler = None

        datasource_config = {
            "name": "postgres",
            "class_name": "Datasource",
            "execution_engine": {"class_name": "PandasExecutionEngine"},
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id"],
                }
            },
        }
        context.test_yaml_config(yaml.dump(datasource_config))
        context.add_datasource(**datasource_config)

        data = pd.DataFrame(
            {"account_id": ["a", "b"], "log_date": ["2022-01-01", "2022-01-02"]}
        )
        batch_request = RuntimeBatchRequest(
            datasource_name="postgres",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name=f"public.result_facebook",
            batch_identifiers={"batch_id": "default_identifier"},
            runtime_parameters={"batch_data": data},
        )

        expectation_suite_name: str = f"test_topic"
        context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name, overwrite_existing=True
        )
        validator = context.get_validator(
            batch_request=batch_request, expectation_suite_name=expectation_suite_name,
        )
        result = getattr(validator, "expect_column_values_to_be_in_set")(
            **{"column": "account_id", "value_set": ["a"]}
        )
        expectation = validator.validate()
        # 由 DataHubValidationAction 重写
        DataHubValidation()._run(
            validation_result_suite=expectation,
            data_asset=validator,
            platform_name="dwd",
        )


if __name__ == "__main__":
    CustomGreatExpectations().run()
