# -*- coding: utf-8 -*-
"""
    Created by wq at 2023/3/1.
    Copyright (c) 2013-present, Xiamen Dianchu Technology Co.,Ltd.
    Description:
    Changelog: all notable changes to this file will be documented
"""
if __name__ == "__main__":
    import great_expectations as ge

    my_df = ge.read_csv("../../data/yellow_tripdata_sample_2019-02.csv")
    result = my_df.expect_column_values_to_be_in_set(
        column="vendor_id",
        value_set=[1],
        row_condition="rate_code_id==1",
        condition_parser="pandas",
    )
    print(result)
