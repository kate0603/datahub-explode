# -*- coding: utf-8 -*-
"""
    Created by w at 2022/5/30.
    Description: https://datahubproject.io/docs/how/delete-metadata/
    Changelog: all notable changes to this file will be documented
"""
import unittest
from example.ingestion.datahub_manage import DatahubManage


class TestDatahubManage(unittest.TestCase):
    """
    """

    def setUp(self) -> None:
        pass

    @unittest.skip("直接跳过测试")
    def test_delete_dataset(self):
        DatahubManage.delete_dataset()


if __name__ == "__main__":
    unittest.main()
