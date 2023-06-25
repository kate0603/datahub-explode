# -*- coding: utf-8 -*-
"""
    Created by w at 2022/5/30.
    Description: https://datahubproject.io/docs/how/delete-metadata/
    Changelog: all notable changes to this file will be documented
"""
import unittest
from example.api.datahub_sdk import DatahubSdk


class TestDatahubSdk(unittest.TestCase):
    """
    """

    def setUp(self) -> None:
        pass

    @unittest.skip("直接跳过测试")
    def test_list_query(self):
        """
        :return: {'listQueries': {'start': 0, 'count': 1000, 'total': 1, 'queries': [{'urn': 'urn:li:query:e161dc32-ca4d-4a9b-bd92-647fc9b463d7', 'type': 'QUERY', 'properties': {'name': 'tt', 'description': 't', 'statement': {'value': 'select * from table', 'language': 'SQL'}}, 'subjects': [{'dataset': {'urn': 'urn:li:dataset:(urn:li:dataPlatform:dwd,dc.dwd_expenditure,PROD)'}}]}]}}
        """
        obj = DatahubSdk()
        data = obj.list_query(
            platform="dwd",
            name="dc.dwd_expenditure",
        )
        print(data)
    @unittest.skip("直接跳过测试")
    def test_batch_set_data_product(self):
        """
        :return:
        """
        obj = DatahubSdk()
        obj.batch_set_data_product(
            dataset_list=[{"platform": "kafka", "name": "chest_game_coin"}], name="wsy"
        )

    @unittest.skip("直接跳过测试")
    def test_create_data_product(self):
        """
        :return:
        """
        obj = DatahubSdk()
        obj.create_data_product(name="test", desc="test", domain_name="运营")

    @unittest.skip("直接跳过测试")
    def test_list_domain(self):
        """
        :return:
        """
        obj = DatahubSdk()
        data = obj.list_domain()
        print(data)

    @unittest.skip("直接跳过测试")
    def test_list_terms(self):
        """
        :return:
        """
        obj = DatahubSdk()
        data = obj.list_terms()
        print(data)

    @unittest.skip("直接跳过测试")
    def test_create_query(self):
        """
        :return: {'createQuery': {'urn': 'urn:li:query:f2fdffe9-9f21-480a-8837-36d6c568858a'}}
        """
        obj = DatahubSdk()
        obj.create_query(
            platform="dwd",
            name="dc.dwd_expenditure",
            query_info={"name": "tt", "description": "t", "sql": "select * from table"},
        )

    @unittest.skip("直接跳过测试")
    def test_delete_query(self):
        """
        :return:
        """
        obj = DatahubSdk()
        obj.delete_query(query_id="e224511c-aa25-4f6b-8770-170cd43e9c64")

    @unittest.skip("直接跳过测试")
    def test_soft_deleted_dataset(self):
        data = DatahubSdk().soft_deleted_dataset(
            dataset_list=[{"platform": "dwd", "name": "dc.dwd_test"}]
        )
        print(data)

    @unittest.skip("直接跳过测试")
    def test_create_dataset(self):
        DatahubSdk().create_dataset(
            platform="dwd",
            name="dc.dwd_test",
            column_info=[
                {"name": "role_id", "desc": "role_test", "type": "varchar"},
                {"name": "log_date", "desc": "date_test", "type": "date"},
            ],
            raw_schema="create table",
        )

    @unittest.skip("直接跳过测试")
    def test_remove_custom_property(self):
        DatahubSdk().remove_custom_property(
            platform="dwd",
            name="dc.dwd_reg_new",
            custom_property_key=["cluster_name", "retention_time"],
        )

    @unittest.skip("直接跳过测试")
    def test_set_custom_property(self):
        DatahubSdk().set_custom_property(
            platform="dwd",
            name="dc.dwd_reg_new",
            custom_property={
                "cluster_name": "datahubproject.acryl.io",
                "retention_time": "2 years",
            },
        )

    @unittest.skip("直接跳过测试")
    def test_add_custom_property(self):
        DatahubSdk().add_custom_property(
            platform="dwd",
            name="dc.dwd_reg_new",
            custom_property={
                "cluster_name": "datahubproject.acryl.io",
                "retention_time": "2 years",
            },
        )

    @unittest.skip("直接跳过测试")
    def test_column_add_description(self):
        DatahubSdk().column_add_description(
            platform="dwd",
            name="dc.dwd_reg_new",
            column="role_id",
            documentation_to_add="Name of the user who was deleted. This description is updated via PythonSDK.",
        )

    @unittest.skip("直接跳过测试")
    def test_add_documentation(self):
        DatahubSdk().add_documentation(
            platform="dwd",
            name="dc.dwd_reg_new",
            documentation_to_add="## The Real Estate Sales Dataset\nThis is a really important Dataset that contains all the relevant information about sales that have happened organized by address.\n",
        )

    @unittest.skip("直接跳过测试")
    def test_add_documentation_link(self):
        DatahubSdk().add_documentation_link(
            platform="dwd",
            name="dc.dwd_reg_new",
            link_to_add="https://wikipedia.com/real_estate",
            link_description="This is the definition of what real estate means",
        )

    @unittest.skip("直接跳过测试")
    def test_read_schema_column(self):
        data = DatahubSdk().read_schema(platform="dwd", name="dc.dwd_reg_new")[
            "schemaMetadata"
        ]

        column_descriptions = [
            {field.fieldPath: field.description} for field in data.fields
        ]
        print(column_descriptions)

    @unittest.skip("直接跳过测试")
    def test_read_schema(self):
        """

        :return:
        """
        data = DatahubSdk().read_schema(platform="dwd", name="dc.dwd_reg_new")
        print(data)

    @unittest.skip("直接跳过测试")
    def test_read_description(self):
        """

        :return:{DatasetPropertiesClass({'customProperties': {'owner': 'he', 'database': 'dc', 'partition_columns': "['game_abbr', 'log_date']", 'data_type': 'parquet', 'location': 'obs://dc-uat-big-data-dw/warehouse/dwd/dwd_reg_new', 'data_location': 'OBS', 'table_type': 'EXTERNAL'}, 'externalUrl': None, 'name': 'dwd_reg_new', 'qualifiedName': None, 'description': None, 'uri': None, 'created': None, 'lastModified': None, 'tags': []})
        """
        data = DatahubSdk().read_properties(platform="dwd", name="dc.dwd_reg_new")
        print(data["datasetProperties"].description)

    @unittest.skip("直接跳过测试")
    def test_read_properties(self):
        """

        :return:{'datasetProperties': DatasetPropertiesClass({'customProperties': {'owner': 'h', 'database': 'dc', 'partition_columns': "['game_abbr', 'log_date']", 'data_type': 'parquet', 'location': 'obs://dwd_reg_new', 'data_location': 'OBS', 'table_type': 'EXTERNAL'}, 'externalUrl': None, 'name': 'dwd_reg_new', 'qualifiedName': None, 'description': None, 'uri': None, 'created': None, 'lastModified': None, 'tags': []})}
        """
        data = DatahubSdk().read_properties(platform="dwd", name="dc.dwd_reg_new")
        print(data)

    @unittest.skip("直接跳过测试")
    def test_batch_add_deprecation(self):
        data = DatahubSdk().batch_add_deprecation(
            dataset_list=[
                {"platform": "dwd", "name": "dc.dwd_login_new"},
                {"platform": "dwd", "name": "dc.dwd_reg_new"},
            ],
            deprecated="false",
        )
        print(data)

    @unittest.skip("直接跳过测试")
    def test_add_deprecation(self):
        """

        :return:{'updateDeprecation': True}
        """
        data = DatahubSdk().add_deprecation(platform="dwd", name="dc.dwd_reg_new")
        print(data)

    @unittest.skip("直接跳过测试")
    def test_read_deprecation(self):
        """

        :return:{'deprecation': DeprecationClass({'deprecated': True, 'decommissionTime': None, 'note': '', 'actor': 'urn:li:corpuser:datahub'})}
        """
        data = DatahubSdk().read_deprecation(platform="dwd", name="dc.dwd_login_new")
        print(data)

    @unittest.skip("直接跳过测试")
    def test_remove_domain(self):
        DatahubSdk().remove_domain(platform="dwd", name="dc.dwd_login_new")

    @unittest.skip("直接跳过测试")
    def test_add_domain(self):
        DatahubSdk().add_domain(platform="dwd", name="dc.dwd_login_new", domain="test")

    @unittest.skip("直接跳过测试")
    def test_read_domain(self):
        """

        :return:{'domains': DomainsClass({'domains': ['urn:li:domain:25ab22d4-3217-46ee-86f8-27bb4673758b']})}
        """
        data = DatahubSdk().read_domain(platform="dwd", name="dc.dwd_login_new")
        print(data)

    @unittest.skip("直接跳过测试")
    def test_create_domain(self):
        DatahubSdk().create_domain(
            domain="test", domain_name="test", domain_desc="dest"
        )

    @unittest.skip("直接跳过测试")
    def test_remove_owners(self):
        """

        :return:{'batchRemoveOwners': True}
        """
        data = DatahubSdk().remove_owners(
            owner_list=["w"],
            dataset_list=[
                {"platform": "dwd", "name": "dc.dwd_login_new"},
                {"platform": "dwd", "name": "dc.dwd_reg_new"},
            ],
        )
        print(data)

    @unittest.skip("直接跳过测试")
    def test_add_owners(self):
        DatahubSdk().add_owners(platform="dwd", name="dc.dwd_login_new", owner="w")

    @unittest.skip("直接跳过测试")
    def test_read_owners(self):
        """

        :return:{'ownership': OwnershipClass({'owners': [OwnerClass({'owner': 'urn:li:corpuser:h', 'type': 'DATAOWNER', 'source': None})], 'lastModified': AuditStampClass({'time': 0, 'actor': 'urn:li:corpuser:unknown', 'impersonator': None, 'message': None})})}
        """
        data = DatahubSdk().read_owners(platform="dwd", name="dc.dwd_reg_new")
        print(data)

    @unittest.skip("直接跳过测试")
    def test_upsert_group(self):
        info = {
            "id": "w",
            "owners": ["ww"],
            "members": ["www"],
            "display_name": "Foo Group",
            "email": "163.com",
            "description": "test",
            "slack": "@foogroup",
        }

        DatahubSdk().upsert_group(group_info=info)

    @unittest.skip("直接跳过测试")
    def test_upsert_user(self):
        info = {
            "id": "w",
            "display_name": "w",
            "email": "163.com",
            "title": "Software Engineer",
            "first_name": "The",
            "last_name": "Bar",
            "full_name": "The Bar",
            "phone": "123",
            "description": "test",
            "country_code": "cn",
            # "groups": "group_1",
        }

        DatahubSdk().upsert_user(user_info=info)

    @unittest.skip("直接跳过测试")
    def test_remove_terms(self):
        """

        :return:{'batchRemoveTerms': True}
        """
        data = DatahubSdk().remove_terms(
            terms_list=["game_terms"],
            dataset_list=[
                {"platform": "dwd", "name": "dc.dwd_login_new"},
                {"platform": "dwd", "name": "dc.dwd_reg_new"},
            ],
        )
        print(data)

    @unittest.skip("直接跳过测试")
    def test_column_add_terms(self):
        DatahubSdk().column_add_terms(
            platform="dwd", name="dc.dwd_reg_new", terms="game_terms", column="role_id"
        )

    @unittest.skip("直接跳过测试")
    def test_add_terms(self):
        DatahubSdk().add_terms(
            platform="dwd", name="dc.dwd_reg_new", terms="game_terms"
        )

    @unittest.skip("直接跳过测试")
    def test_read_terms(self):
        """

        :return:{'glossaryTerms': GlossaryTermsClass({'terms': [GlossaryTermAssociationClass({'urn': 'urn:li:glossaryTerm:chest', 'context': None})], 'auditStamp': AuditStampClass({'time': 1685092243230, 'actor': 'urn:li:corpuser:datahub', 'impersonator': None, 'message': None})})}
        """
        data = DatahubSdk().read_terms(platform="dwd", name="dc.dwd_reg_new")
        print(data)

    @unittest.skip("直接跳过测试")
    def test_create_terms(self):
        DatahubSdk().create_terms(terms_name="game_terms", terms_desc="test")

    @unittest.skip("直接跳过测试")
    def test_column_remove_tags(self):
        """

        :return:{'removeTag': True}
        """
        data = DatahubSdk().column_remove_tags(
            platform="dwd", name="dc.dwd_reg_new", tag="已下线", column="role_id"
        )
        print(data)

    @unittest.skip("直接跳过测试")
    def test_column_add_tags(self):
        """

        :return:
        """
        DatahubSdk().column_add_tags(
            platform="dwd", name="dc.dwd_reg_new", tag="已下线", column="role_id"
        )

    @unittest.skip("直接跳过测试")
    def test_add_tags(self):
        """

        :return:
        """
        DatahubSdk().add_tags(platform="dwd", name="dc.dwd_reg_new", tag="已下线")

    @unittest.skip("直接跳过测试")
    def test_read_tags(self):
        """

        :return:{'globalTags': GlobalTagsClass({'tags': [TagAssociationClass({'tag': 'urn:li:tag:在线', 'context': None})]})}
        """
        data = DatahubSdk().read_tags(platform="dwd", name="dc.dwd_reg_new")
        print(data)

    @unittest.skip("直接跳过测试")
    def test_create_tags(self):
        DatahubSdk().create_tags(tag_name="在线", tag_desc="test")

    @unittest.skip("直接跳过测试")
    def test_lineage_column(self):
        lineage = [
            {
                "down": {"column": "role_id"},
                "ups": [
                    {"platform": "ods", "name": "gos_src_reg", "column": "role_id"},
                    {"platform": "ods", "name": "wsy_src_reg", "column": "role_id"},
                ],
            },
            {
                "down": {"column": "server_id"},
                "ups": [
                    {"platform": "ods", "name": "gos_src_reg", "column": "server_id"},
                    {"platform": "ods", "name": "wsy_src_reg", "column": "server_id"},
                ],
            },
        ]
        down = {"platform": "dwd", "name": "dc.dwd_reg_new"}
        DatahubSdk().make_lineage_column(lineage_info=lineage, down=down)

    @unittest.skip("直接跳过测试")
    def test_lineage(self):
        lineage = {
            "down": {"platform": "dwd", "name": "dc.dwd_reg_new"},
            "ups": [
                {"platform": "ods", "name": "gos_src_reg"},
                {"platform": "ods", "name": "wsy_src_reg"},
            ],
        }
        DatahubSdk().make_lineage(lineage_info=lineage)


if __name__ == "__main__":
    unittest.main()
