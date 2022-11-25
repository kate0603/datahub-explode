# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/17.
    Description: 文档 https://datahubproject.io/docs/generated/ingestion/sources/postgres
    Changelog: all notable changes to this file will be documented
"""
from example.config import pg_db, datahub_server
from datahub.ingestion.run.pipeline import Pipeline


def run_pg():
    # The pipeline configuration is similar to the recipe YAML files provided to the CLI tool.
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "postgres",
                "config": {
                    "username": pg_db["username"],
                    "password": pg_db["password"],
                    "database": pg_db["database"],
                    "host_port": f"{pg_db['host']}:{pg_db['port']}",
                    "schema_pattern": {"allow": ["public"]},
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {
                    "server": datahub_server["server"],
                    "token": datahub_server["token"],
                },
            },
        }
    )


    # Run the pipeline and report the results.
    pipeline.run()
    pipeline.pretty_print_summary()


if __name__ == "__main__":
    run_pg()
