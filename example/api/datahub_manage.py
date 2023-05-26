# -*- coding: utf-8 -*-
"""
    Created by w at 2022/5/30.
    Description: https://datahubproject.io/docs/how/delete-metadata/
    Changelog: all notable changes to this file will be documented
"""
import json
import requests


class DatahubManage(object):
    """
    """
    @classmethod
    def delete_dataset(cls):
        config: dict = {"server": "http://127.0.0.1", "token": ""}
        token = config.get("token")
        server = config.get("server")
        urn = f"urn:li:dataset:(urn:li:dataPlatform:dwd,dc.dwd_chat,PROD)"
        url = f"{server}:9002/openapi/entities/v1/?urns={urn}&soft=false"
        token = f"Bearer {token}"

        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": token,
        }

        response = requests.delete(url, headers=headers)
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
