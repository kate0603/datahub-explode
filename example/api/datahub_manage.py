# -*- coding: utf-8 -*-
"""
    Created by w at 2022/5/30.
    Description: https://datahubproject.io/docs/how/delete-metadata/
    Changelog: all notable changes to this file will be documented
"""
import json
import requests
from example.config import datahub_server
from datahub.emitter.mce_builder import dataset_key_to_urn
from datahub.metadata.schema_classes import DatasetKeyClass


class DatahubManage(object):
    """
    """

    def __init__(self):
        token: str = datahub_server.get("token")
        self.server: str = datahub_server.get("openapi")
        self.env: str = "PROD"
        token = f"Bearer {token}"
        self.headers: dict = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": token,
        }

    def delete_dataset(self, platform: str, name: str):
        urn: str = dataset_key_to_urn(
            key=DatasetKeyClass(platform=platform, name=name, origin=self.env)
        )
        url = f"{self.server}/entities/v1/?urns={urn}&soft=false"

        response = requests.delete(url, headers=self.headers)
        # 检查响应代码，如果成功则输出 URN 已被删除
        if response.status_code == 200:
            res = response.json()
            print(res)
            print(f"URN {urn} has been deleted.")
        else:
            print(f"Failed to delete URN {urn}.")
            # 输出错误信息
            error_info = json.loads(response.text)
            print(error_info["message"])
