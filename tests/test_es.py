# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/29.
    Description:
    Changelog: all notable changes to this file will be documented
"""
import unittest
import json
from requests import request
from example.config import uat_kibana_host


class TestEs(unittest.TestCase):
    """
    """

    def setUp(self) -> None:
        pass

    @unittest.skip("直接跳过测试")
    def test_kibana_list(self):
        base_url = uat_kibana_host
        url: str = f"{base_url}/api/saved_objects/_find"

        headers: dict = {
            "Content-Type": "application/json",
            "kbn-xsrf": "True",
        }
        params = {
            "type": "index-pattern",
            "page": 1,
            "per_page": 1,
        }
        index_pattern_response = request(
            headers=headers, url=url, method="get", params=params
        )
        index_pattern_json = index_pattern_response.json()
        print(index_pattern_json)

    @unittest.skip("直接跳过测试")
    def test_kibana_create(self):
        index_pattern: str = "myth"
        base_url = uat_kibana_host
        url: str = f"{base_url}/api/saved_objects/index-pattern/{index_pattern}?overwrite=false"

        headers: dict = {
            "Content-Type": "application/json",
            "kbn-xsrf": "True",
        }
        index_pattern_title: str = "myth*"
        data = {
            "attributes": {
                "title": index_pattern_title,
                "timeFieldName": "generated_time",
            }
        }

        index_pattern_response = request(
            headers=headers, url=url, method="post", data=json.dumps(data)
        )
        index_pattern_json = index_pattern_response.json()
        if index_pattern_json["statusCode"] == 409:
            msg = f"{index_pattern_json['error']},{index_pattern_json['message']}"
        print(index_pattern_json)

    @unittest.skip("直接跳过测试")
    def test_kibana(self):
        from kibana_api import Kibana

        client = Kibana(base_url=uat_kibana_host)
        objects_response = client.object().all(type="index-pattern")  # Type in specific
        objects_json = objects_response.json()
        print(objects_json)
        pattern_json = {
            "title": "myth*",
            "timeFieldName": "generated_time",  # timefiledname is important, it taken as a reference to time
            "fields": "[]",
        }
        index_pattern_response = client.object().create(
            "index-pattern", attribs=pattern_json
        )
        index_pattern_json = index_pattern_response.json()
        print(index_pattern_json)

    @unittest.skip("直接跳过测试")
    def test_es(self):
        from elasticsearch import Elasticsearch
        from example.config import kibana_host

        client = Elasticsearch(hosts=kibana_host)
        index = "myth_sched_log_tag-2022.12.06-reindex"
        # client.indices.create(index=index)
        # data = client.indices.get(index=index)
        # data = client.indices.exists(index=index)
        # print(data)
        # client.indices.delete(index=index)
        # indices = client.indices.get_alias()
        # print(indices)
        # if index in indices:
        #     print(1)
        # else:
        #     print(2)
        # for index in indices:
        #     print(index, client.indices.exists(index=index))
        # if not client.indices.exists(index=index):
        #     continue
        # data = client.indices.get(index="wsy_src_chat-2022.12.06-reindex")
        # 模板
        # data = client.indices.get_template()


if __name__ == "__main__":
    unittest.main()
