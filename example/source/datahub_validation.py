# -*- coding: utf-8 -*-
"""
    Created by w at 2022/4/6.
    Description:
    Changelog: all notable changes to this file will be documented
"""
import json
import logging
import time
from dataclasses import dataclass
from datetime import timezone
from typing import Any, Dict, List, Optional, Union
from example.config import datahub_server

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.data_asset.data_asset import DataAsset
from great_expectations.validator.validator import Validator
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    BatchSpec,
    DatasetAssertionInfo,
    DatasetAssertionScope,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.com.linkedin.pegasus2avro.events.metadata import ChangeType

logger = logging.getLogger(__name__)


class DataHubValidation(object):
    def __init__(
        self,
        env: str = builder.DEFAULT_ENV,
        platform_instance_map: Optional[Dict[str, str]] = None,
        graceful_exceptions: bool = True,
        token: Optional[str] = None,
        timeout_sec: Optional[float] = None,
        retry_status_codes: Optional[List[int]] = None,
        retry_max_times: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        parse_table_names_from_sql: bool = False,
    ):
        self.server_url = datahub_server["server"]
        self.env = env
        self.platform_instance_map = platform_instance_map
        self.graceful_exceptions = graceful_exceptions
        self.token = token or datahub_server["token"]
        self.timeout_sec = timeout_sec
        self.retry_status_codes = retry_status_codes
        self.retry_max_times = retry_max_times
        self.extra_headers = extra_headers
        self.parse_table_names_from_sql = parse_table_names_from_sql

    def _run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        data_asset: Union[Validator, DataAsset, Batch],
        platform_name: str,
        payload: Any = None,
    ) -> Dict:
        try:
            self.ge_platform_name = platform_name
            emitter = DatahubRestEmitter(
                gms_server=self.server_url,
                token=self.token,
                read_timeout_sec=self.timeout_sec,
                connect_timeout_sec=self.timeout_sec,
                retry_status_codes=self.retry_status_codes,
                retry_max_times=self.retry_max_times,
                extra_headers=self.extra_headers,
            )

            expectation_suite_name = validation_result_suite.meta.get(
                "expectation_suite_name"
            )
            run_id = validation_result_suite.meta.get("run_id")
            if hasattr(data_asset, "active_batch_id"):
                batch_identifier = data_asset.active_batch_id
            else:
                batch_identifier = data_asset.batch_id

            # Returns datasets and corresponding batch requests
            datasets = self.get_dataset_partitions(batch_identifier, data_asset)
            # print("dataset==", data_asset)

            if len(datasets) == 0 or datasets[0]["dataset_urn"] is None:
                logger.info("Metadata not sent to datahub. No datasets found.")
                return {"datahub_notification_result": "none required"}

            # Returns assertion info and assertion results
            assertions = self.get_assertions_with_results(
                validation_result_suite,
                expectation_suite_name,
                run_id,
                payload,
                datasets,
            )
            # print("assetions==", assertions)

            for assertion in assertions:
                # print("asset==", assertion)
                # Construct a MetadataChangeProposalWrapper object.
                assertion_info_mcp = MetadataChangeProposalWrapper(
                    entityType="assertion",
                    changeType=ChangeType.UPSERT,
                    entityUrn=assertion["assertionUrn"],
                    aspectName="assertionInfo",
                    aspect=assertion["assertionInfo"],
                )
                emitter.emit_mcp(assertion_info_mcp)

                # Construct a MetadataChangeProposalWrapper object.
                assertion_platform_mcp = MetadataChangeProposalWrapper(
                    entityType="assertion",
                    changeType=ChangeType.UPSERT,
                    entityUrn=assertion["assertionUrn"],
                    aspectName="dataPlatformInstance",
                    aspect=assertion["assertionPlatform"],
                )
                emitter.emit_mcp(assertion_platform_mcp)

                for assertionResult in assertion["assertionResults"]:
                    dataset_assertionResult_mcp = MetadataChangeProposalWrapper(
                        entityType="assertion",
                        changeType=ChangeType.UPSERT,
                        entityUrn=assertionResult.assertionUrn,
                        aspectName="assertionRunEvent",
                        aspect=assertionResult,
                    )

                    # Emit Result! (timseries aspect)
                    emitter.emit_mcp(dataset_assertionResult_mcp)

            result = "DataHub notification succeeded"
        except Exception as e:
            result = "DataHub notification failed"
            if self.graceful_exceptions:
                logger.error(e)
                logger.info("Supressing error because graceful_exceptions is set")
            else:
                print("datahub err===", e)

        return {"datahub_notification_result": result}

    def get_assertions_with_results(
        self,
        validation_result_suite,
        expectation_suite_name,
        run_id,
        payload,
        datasets,
    ):

        dataPlatformInstance = DataPlatformInstance(
            platform=builder.make_data_platform_urn(self.ge_platform_name)
        )
        docs_link = None
        if payload:
            # process the payload
            for action_names in payload.keys():
                if payload[action_names]["class"] == "UpdateDataDocsAction":
                    data_docs_pages = payload[action_names]
                    for docs_link_key, docs_link_val in data_docs_pages.items():
                        if "file://" not in docs_link_val and docs_link_key != "class":
                            docs_link = docs_link_val

        assertions_with_results = []
        for result in validation_result_suite.results:
            expectation_config = result["expectation_config"]
            expectation_type = expectation_config["expectation_type"]
            success = True if result["success"] else False
            kwargs = {
                k: v for k, v in expectation_config["kwargs"].items() if k != "batch_id"
            }

            result = result["result"]
            assertion_datasets = [d["dataset_urn"] for d in datasets]
            if len(datasets) == 1 and "column" in kwargs:
                assertion_fields = [
                    builder.make_schema_field_urn(
                        datasets[0]["dataset_urn"], kwargs["column"]
                    )
                ]
            else:
                assertion_fields = None  # type:ignore

            # Be careful what fields to consider for creating assertion urn.
            # Any change in fields below would lead to a new assertion
            # FIXME - Currently, when using evaluation parameters, new assertion is
            # created when runtime resolved kwargs are different,
            # possibly for each validation run
            # todo  在datahub-cli删除了urn，会导致同一个urn无法更新
            assertionUrn = builder.make_assertion_urn(
                builder.datahub_guid(
                    {
                        "platform": self.ge_platform_name,
                        "nativeType": expectation_type,
                        "nativeParameters": kwargs,
                        "dataset": assertion_datasets[0],
                        "fields": assertion_fields,
                        # "version": "1.0",
                    }
                )
            )
            # print("assertionUrn===", assertionUrn)
            assertionInfo: AssertionInfo = self.get_assertion_info(
                expectation_type,
                kwargs,
                assertion_datasets[0],
                assertion_fields,
                expectation_suite_name,
            )

            # TODO: Understand why their run time is incorrect.
            run_time = run_id.run_time.astimezone(timezone.utc)
            assertionResults = []

            evaluation_parameters = (
                {
                    k: convert_to_string(v)
                    for k, v in validation_result_suite.evaluation_parameters.items()
                }
                if validation_result_suite.evaluation_parameters
                else None
            )

            nativeResults = {
                k: convert_to_string(v)
                for k, v in result.items()
                if (
                    k
                    in [
                        "observed_value",
                        "partial_unexpected_list",
                        "partial_unexpected_counts",
                        "details",
                    ]
                    and v
                )
            }

            actualAggValue = (
                result.get("observed_value")
                if isinstance(result.get("observed_value"), (int, float))
                else None
            )

            ds = datasets[0]
            # https://docs.greatexpectations.io/docs/reference/expectations/result_format/
            assertionResult = AssertionRunEvent(
                timestampMillis=int(round(time.time() * 1000)),
                assertionUrn=assertionUrn,
                asserteeUrn=ds["dataset_urn"],
                runId=run_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                result=AssertionResult(
                    type=AssertionResultType.SUCCESS
                    if success
                    else AssertionResultType.FAILURE,
                    rowCount=result.get("element_count"),
                    missingCount=result.get("missing_count"),
                    unexpectedCount=result.get("unexpected_count"),
                    actualAggValue=actualAggValue,
                    externalUrl=docs_link,
                    nativeResults=nativeResults,
                ),
                batchSpec=ds["batchSpec"],
                status=AssertionRunStatus.COMPLETE,
                runtimeContext=evaluation_parameters,
            )
            if ds.get("partitionSpec") is not None:
                assertionResult.partitionSpec = ds.get("partitionSpec")
            assertionResults.append(assertionResult)

            assertions_with_results.append(
                {
                    "assertionUrn": assertionUrn,
                    "assertionInfo": assertionInfo,
                    "assertionPlatform": dataPlatformInstance,
                    "assertionResults": assertionResults,
                }
            )
        return assertions_with_results

    def get_assertion_info(
        self, expectation_type, kwargs, dataset, fields, expectation_suite_name
    ):

        # TODO - can we find exact type of min and max value
        def get_min_max(kwargs, type=AssertionStdParameterType.UNKNOWN):
            return AssertionStdParameters(
                minValue=AssertionStdParameter(
                    value=convert_to_string(kwargs.get("min_value")), type=type,
                ),
                maxValue=AssertionStdParameter(
                    value=convert_to_string(kwargs.get("max_value")), type=type,
                ),
            )

        known_expectations: Dict[str, DataHubStdAssertion] = {
            # column aggregate expectations
            "expect_column_min_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation.MIN,
                parameters=get_min_max(kwargs),
            ),
            "expect_column_max_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation.MAX,
                parameters=get_min_max(kwargs),
            ),
            "expect_column_median_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation.MEDIAN,
                parameters=get_min_max(kwargs),
            ),
            "expect_column_stdev_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation.STDDEV,
                parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
            ),
            "expect_column_mean_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation.MEAN,
                parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
            ),
            "expect_column_unique_value_count_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation.UNIQUE_COUNT,
                parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
            ),
            "expect_column_proportion_of_unique_values_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation.UNIQUE_PROPOTION,
                parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
            ),
            "expect_column_sum_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation.SUM,
                parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
            ),
            "expect_column_quantile_values_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation._NATIVE_,
            ),
            # column map expectations
            "expect_column_values_to_not_be_null": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.NOT_NULL,
                aggregation=AssertionStdAggregation.IDENTITY,
            ),
            "expect_column_values_to_be_in_set": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.IN,
                aggregation=AssertionStdAggregation.IDENTITY,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value=convert_to_string(kwargs.get("value_set")),
                        type=AssertionStdParameterType.SET,
                    )
                ),
            ),
            "expect_column_values_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation.IDENTITY,
                parameters=get_min_max(kwargs),
            ),
            "expect_column_values_to_match_regex": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.REGEX_MATCH,
                aggregation=AssertionStdAggregation.IDENTITY,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value=kwargs.get("regex"),
                        type=AssertionStdParameterType.STRING,
                    )
                ),
            ),
            "expect_column_values_to_match_regex_list": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                operator=AssertionStdOperator.REGEX_MATCH,
                aggregation=AssertionStdAggregation.IDENTITY,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value=convert_to_string(kwargs.get("regex_list")),
                        type=AssertionStdParameterType.LIST,
                    )
                ),
            ),
            "expect_table_columns_to_match_ordered_list": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_SCHEMA,
                operator=AssertionStdOperator.EQUAL_TO,
                aggregation=AssertionStdAggregation.COLUMNS,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value=convert_to_string(kwargs.get("column_list")),
                        type=AssertionStdParameterType.LIST,
                    )
                ),
            ),
            "expect_table_columns_to_match_set": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_SCHEMA,
                operator=AssertionStdOperator.EQUAL_TO,
                aggregation=AssertionStdAggregation.COLUMNS,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value=convert_to_string(kwargs.get("column_set")),
                        type=AssertionStdParameterType.SET,
                    )
                ),
            ),
            "expect_table_column_count_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_SCHEMA,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation.COLUMN_COUNT,
                parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
            ),
            "expect_table_column_count_to_equal": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_SCHEMA,
                operator=AssertionStdOperator.EQUAL_TO,
                aggregation=AssertionStdAggregation.COLUMN_COUNT,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value=convert_to_string(kwargs.get("value")),
                        type=AssertionStdParameterType.NUMBER,
                    )
                ),
            ),
            "expect_column_to_exist": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_SCHEMA,
                operator=AssertionStdOperator._NATIVE_,
                aggregation=AssertionStdAggregation._NATIVE_,
            ),
            "expect_table_row_count_to_equal": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_ROWS,
                operator=AssertionStdOperator.EQUAL_TO,
                aggregation=AssertionStdAggregation.ROW_COUNT,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value=convert_to_string(kwargs.get("value")),
                        type=AssertionStdParameterType.NUMBER,
                    )
                ),
            ),
            "expect_table_row_count_to_be_between": DataHubStdAssertion(
                scope=DatasetAssertionScope.DATASET_ROWS,
                operator=AssertionStdOperator.BETWEEN,
                aggregation=AssertionStdAggregation.ROW_COUNT,
                parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
            ),
        }

        datasetAssertionInfo = DatasetAssertionInfo(
            dataset=dataset,
            fields=fields,
            operator=AssertionStdOperator._NATIVE_,
            aggregation=AssertionStdAggregation._NATIVE_,
            nativeType=expectation_type,
            nativeParameters={k: convert_to_string(v) for k, v in kwargs.items()},
            scope=DatasetAssertionScope.DATASET_ROWS,
        )

        if expectation_type in known_expectations.keys():
            assertion = known_expectations[expectation_type]
            datasetAssertionInfo.scope = assertion.scope
            datasetAssertionInfo.aggregation = assertion.aggregation
            datasetAssertionInfo.operator = assertion.operator
            datasetAssertionInfo.parameters = assertion.parameters

        # Heuristically mapping other expectations
        else:
            if "column" in kwargs and expectation_type.startswith(
                "expect_column_value"
            ):
                datasetAssertionInfo.scope = DatasetAssertionScope.DATASET_COLUMN
                datasetAssertionInfo.aggregation = AssertionStdAggregation.IDENTITY
            elif "column" in kwargs:
                datasetAssertionInfo.scope = DatasetAssertionScope.DATASET_COLUMN
                datasetAssertionInfo.aggregation = AssertionStdAggregation._NATIVE_

        return AssertionInfo(
            type=AssertionType.DATASET,
            datasetAssertion=datasetAssertionInfo,
            customProperties={"expectation_suite_name": expectation_suite_name},
        )

    def get_dataset_partitions(self, batch_identifier, data_asset):
        # print("=============", batch_identifier, data_asset)
        dataset_partitions = []

        ge_batch_spec = data_asset.active_batch_spec
        # print("ge_batch_spec==", ge_batch_spec)
        # print("active_batch_definition==", data_asset.active_batch_definition)
        partitionSpec = None
        batchSpecProperties = {
            "data_asset_name": str(data_asset.active_batch_definition.data_asset_name),
            "datasource_name": str(data_asset.active_batch_definition.datasource_name),
        }
        # todo
        dataset_urn = make_dataset_urn(
            self.ge_platform_name,
            str(data_asset.active_batch_definition.data_asset_name),
            self.env,
            self.get_platform_instance(
                data_asset.active_batch_definition.datasource_name
            ),
        )
        batchSpec = BatchSpec(
            nativeBatchId=batch_identifier, customProperties=batchSpecProperties,
        )

        dataset_partitions.append(
            {
                "dataset_urn": dataset_urn,
                "partitionSpec": partitionSpec,
                "batchSpec": batchSpec,
            }
        )
        return dataset_partitions

    def get_platform_instance(self, datasource_name):
        if self.platform_instance_map and datasource_name in self.platform_instance_map:
            return self.platform_instance_map[datasource_name]
        if self.platform_instance_map:
            warn(
                f"Datasource {datasource_name} is not present in platform_instance_map"
            )
        return None


def make_dataset_urn(data_platform, dataset_name, env, platform_instance=None):
    """
    f"urn:li:dataset:({f"urn:li:dataPlatform:{platform}"},{platform_instance}.{name},{env})"
    :param data_platform:
    :param dataset_name:
    :param env:
    :param platform_instance:
    :return:
    """
    dataset_urn = builder.make_dataset_urn_with_platform_instance(
        platform=data_platform,
        name=dataset_name,
        platform_instance=platform_instance,
        env=env,
    )

    return dataset_urn


@dataclass
class DataHubStdAssertion:
    scope: Union[str, DatasetAssertionScope]
    operator: Union[str, AssertionStdOperator]
    aggregation: Union[str, AssertionStdAggregation]
    parameters: Optional[AssertionStdParameters] = None


def convert_to_string(var):
    import numpy

    return (
        str(var) if isinstance(var, (str, int, float, numpy.int64)) else json.dumps(var)
    )


def warn(msg):
    logger.warning(msg)
