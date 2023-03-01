# -*- coding: utf-8 -*-
"""
    Created by wq at 2023/3/1.
    Copyright (c) 2013-present, Xiamen Dianchu Technology Co.,Ltd.
    Description:
    Changelog: all notable changes to this file will be documented
"""
import os
import copy
import pandas as pd
from ruamel import yaml
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
    DataContextConfigDefaults,
)
from great_expectations.data_context import BaseDataContext
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)


class ExampleCheckPoint(object):
    def __init__(self):
        # 数据源名称
        self.datasource_name: str = "dwd"
        self.runtime_connector_name: str = "default_runtime_data_connector_name"
        self.expectation_suite_name: str = "suite_test"
        self.inferred_connector_name: str = "inferred_connector_test"
        self.template_file: str = "template_test"

    def data_source(self):
        """
        数据源
        :return:
        """
        # 数据上下文
        context_root_dir = os.getcwd()
        store_backend_defaults = InMemoryStoreBackendDefaults()
        store_backend_defaults.data_docs_sites = copy.deepcopy(
            DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITES.value
        )
        data_context_config = DataContextConfig(
            store_backend_defaults=store_backend_defaults,
            checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
        )
        self.context = BaseDataContext(
            project_config=data_context_config, context_root_dir=context_root_dir
        )

        source_config = {
            "name": self.datasource_name,
            "class_name": "Datasource",
            "execution_engine": {"class_name": "PandasExecutionEngine"},
            "data_connectors": {
                self.inferred_connector_name: {
                    "class_name": "InferredAssetFilesystemDataConnector",
                    "base_directory": "..\data",
                    "default_regex": {
                        "group_names": ["data_asset_name"],
                        "pattern": "(.*)",
                    },
                },
                self.runtime_connector_name: {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
            },
        }
        self.context.test_yaml_config(yaml.dump(source_config))
        self.context.add_datasource(**source_config)

    def get_data_1(self):
        data = pd.read_csv("..\..\data\yellow_tripdata_sample_2019-02.csv")
        return data

    def suite(self):
        data = self.get_data_1()
        batch_request = {
            "datasource_name": self.datasource_name,
            "data_connector_name": self.runtime_connector_name,
            "data_asset_name": f"{self.template_file}",
            "batch_identifiers": {"default_identifier_name": "default_identifier"},
            "runtime_parameters": {"batch_data": data},
        }

        self.context.create_expectation_suite(
            expectation_suite_name=self.expectation_suite_name, overwrite_existing=True
        )

        validator = self.context.get_validator(
            # batch_request=BatchRequest(**batch_request),
            batch_request=RuntimeBatchRequest(**batch_request),
            expectation_suite_name=self.expectation_suite_name,
        )

        ignored_columns = ["log_date"]

        profiler = UserConfigurableProfiler(
            profile_dataset=validator,
            excluded_expectations=None,
            ignored_columns=ignored_columns,
            not_null_only=False,
            primary_or_compound_key=None,
            semantic_types_dict=None,
            table_expectations_only=False,
            value_set_threshold="MANY",
        )
        suite = profiler.build_suite()
        validator.expectation_suite = suite
        # print('==========')
        # print(validator.get_expectation_suite(discard_failed_expectations=False))
        validator.save_expectation_suite(discard_failed_expectations=False)

    def check_1(self):
        # 加载内存方法1
        checkpoint_config = {
            "name": "my_missing_keys_checkpoint",
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": self.datasource_name,
                        "data_connector_name": self.runtime_connector_name,
                        "data_asset_name": self.template_file,
                    },
                    "expectation_suite_name": self.expectation_suite_name,
                }
            ],
        }
        self.context.add_checkpoint(**checkpoint_config)
        df = self.get_data_1()
        results = self.context.run_checkpoint(
            checkpoint_name="my_missing_keys_checkpoint",
            batch_request={
                "runtime_parameters": {"batch_data": df},
                "batch_identifiers": {"default_identifier_name": "batch_id"},
            },
        )

    def check_2(self):
        # 加载内存方法2
        checkpoint_config = {
            "name": "my_missing_batch_request_checkpoint",
            "run_name_template": "template",
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
            "expectation_suite_name": self.expectation_suite_name,
        }
        self.context.add_checkpoint(**checkpoint_config)
        df = self.get_data_1()

        batch_request = RuntimeBatchRequest(
            datasource_name=self.datasource_name,
            data_connector_name=self.runtime_connector_name,
            data_asset_name=self.template_file,  # This can be anything that identifies this data_asset for you
            runtime_parameters={"batch_data": df},  # Pass your DataFrame here.
            batch_identifiers={"default_identifier_name": "batch_id"},
        )

        results = self.context.run_checkpoint(
            checkpoint_name="my_missing_batch_request_checkpoint",
            validations=[{"batch_request": batch_request},],
        )

    def run_1(self):
        self.data_source()
        self.suite()
        self.check_1()

    def run_2(self):
        self.data_source()
        self.suite()
        self.check_2()


if __name__ == "__main__":
    ExampleCheckPoint().run_2()
