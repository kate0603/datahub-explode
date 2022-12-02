# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/29.
    Description:
    Changelog: all notable changes to this file will be documented
"""
import unittest
from example.graphsql_api.query import DataHubQuery


class TestGraphApi(unittest.TestCase):
    """
    """

    def setUp(self) -> None:
        pass

    @unittest.skip("直接跳过测试")
    def test_browse_list(self):
        data = DataHubQuery().get_browse_list(
            data_type="DATASET", path=["prod", "kafka"], start=0, count=10
        )
        print(data)

    @unittest.skip("直接跳过测试")
    def test_root_glossary_terms(self):
        data = DataHubQuery().get_root_glossary_terms()
        print(data)


if __name__ == "__main__":
    unittest.main()
