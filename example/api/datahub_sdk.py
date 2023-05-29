# -*- coding: utf-8 -*-
"""
    Created by w at 2023/5/23.
    Description:https://datahubproject.io/docs/python-sdk/clients
    Changelog: all notable changes to this file will be documented
"""
import time
from typing import Dict, List, Optional
from example.config import datahub_server
from datahub.api.entities.corpuser.corpuser import CorpUser, CorpUserGenerationConfig
from datahub.api.entities.corpgroup.corpgroup import (
    CorpGroup,
    CorpGroupGenerationConfig,
)
from datahub.emitter.mce_builder import (
    make_tag_urn,
    make_dataset_urn,
    make_schema_field_urn,
    make_lineage_mce,
    make_term_urn,
    make_user_urn,
    make_domain_urn,
    dataset_key_to_urn,
    make_data_platform_urn,
    Aspect,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.utilities.urns.corpuser_urn import CorpuserUrn
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import (
    TagPropertiesClass,
    GlobalTagsClass,
    TagAssociationClass,
    EditableSchemaMetadataClass,
    EditableSchemaFieldInfoClass,
    AuditStampClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    GlossaryTermAssociationClass,
    OwnershipClass,
    OwnershipTypeClass,
    OwnerClass,
    ChangeTypeClass,
    DomainPropertiesClass,
    DomainsClass,
    DatasetKeyClass,
    DeprecationClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    EditableDatasetPropertiesClass,
    InstitutionalMemoryMetadataClass,
    InstitutionalMemoryClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    TimeTypeClass,
    DateTypeClass,
    StringTypeClass,
    BooleanTypeClass,
)
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DatahubSdk(object):
    def __init__(self):
        config: Dict = {
            "gms_server": datahub_server["server"],
            "token": datahub_server["token"],
        }
        self.env: str = "PROD"
        self.emitter = DatahubRestEmitter(**config)
        self.graph = DataHubGraph(
            DatahubClientConfig(server=config["gms_server"], token=config["token"])
        )

    def soft_deleted_dataset(self, dataset_list: List[Dict]) -> Dict:
        """
        软删除
        :param dataset_list:
        :return:
        """
        urns: List = [
            f"""{dataset_key_to_urn(key=DatasetKeyClass(platform=i['platform'], name=i['name'], origin=self.env))}"""
            for i in dataset_list
        ]

        query: str = f"""
        mutation batchUpdateSoftDeleted {{
            batchUpdateSoftDeleted(input:
              {{ urns: ["{','.join(urns)}"],
                deleted: true }})
        }}
        """
        result = self.graph.execute_graphql(query=query)
        return result

    def create_dataset(
        self, platform: str, name: str, column_info: List[Dict], raw_schema: str
    ) -> None:
        """
        创建数据集
        :param platform:
        :param name:
        :param column_info:
        :param raw_schema:
        :return:
        """

        def string_type(x):
            if x in ["timestamp"]:
                return TimeTypeClass()
            if x in ["date"]:
                return DateTypeClass()
            if x in ["bool"]:
                return BooleanTypeClass()
            if x.startswith("varchar"):
                return StringTypeClass()

        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)
        platform_urn: str = make_data_platform_urn(platform=platform)
        now: int = int(time.time() * 1000)
        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SchemaMetadataClass(
                schemaName="customer",  # not used
                platform=platform_urn,  # important <- platform must be an urn
                version=0,  # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
                hash="",  # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
                platformSchema=OtherSchemaClass(rawSchema=raw_schema),
                lastModified=AuditStampClass(
                    time=now, actor="urn:li:corpuser:ingestion"
                ),
                fields=[
                    SchemaFieldClass(
                        fieldPath=i["name"],
                        type=SchemaFieldDataTypeClass(type=string_type(i["type"])),
                        nativeDataType=i[
                            "type"
                        ],  # use this to provide the type of the field in the source system's vernacular
                        description=i["desc"],
                        lastModified=AuditStampClass(
                            time=now, actor="urn:li:corpuser:ingestion",
                        ),
                    )
                    for i in column_info
                ],
            ),
        )
        self.emitter.emit(event)

    def remove_custom_property(
        self, platform: str, name: str, custom_property_key: List
    ) -> None:
        """
        移除自定义属性
        :param platform:
        :param name:
        :param custom_property_key:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)
        builder = DatasetPatchBuilder(dataset_urn)
        for key in custom_property_key:
            builder.remove_custom_property(key=key)

        for patch_mcp in builder.build():
            self.emitter.emit(patch_mcp)

    def set_custom_property(
        self, platform: str, name: str, custom_property: Dict
    ) -> None:
        """
        增加自定义属性
        :param platform:
        :param name:
        :param custom_property:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)
        builder = DatasetPatchBuilder(dataset_urn)
        builder.set_custom_properties(custom_properties=custom_property)

        for patch_mcp in builder.build():
            self.emitter.emit(patch_mcp)
        log.info(
            f"Replaced custom properties on dataset {dataset_urn} as {custom_property}"
        )

    def add_custom_property(
        self, platform: str, name: str, custom_property: Dict
    ) -> None:
        """
        增加自定义属性
        :param platform:
        :param name:
        :param custom_property:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)
        builder = DatasetPatchBuilder(dataset_urn)
        for key, value in custom_property.items():
            builder.add_custom_property(key=key, value=value)

        for patch_mcp in builder.build():
            self.emitter.emit(patch_mcp)

    def column_add_description(
        self, platform: str, name: str, column: str, documentation_to_add: str
    ) -> None:
        """
        增加description至列
        :param platform:
        :param name:
        :param column:
        :param documentation_to_add:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)
        field_info_to_set = EditableSchemaFieldInfoClass(
            fieldPath=column, description=documentation_to_add
        )

        # Some helpful variables to fill out objects later
        now = int(time.time() * 1000)  # milliseconds since epoch
        current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")

        current_editable_schema_metadata = self.graph.get_aspect(
            entity_urn=dataset_urn, aspect_type=EditableSchemaMetadataClass,
        )

        need_write = False

        if current_editable_schema_metadata:
            for fieldInfo in current_editable_schema_metadata.editableSchemaFieldInfo:
                if (
                    self.get_simple_field_path_from_v2_field_path(fieldInfo.fieldPath)
                    == column
                ):
                    # we have some editable schema metadata for this field
                    field_match = True
                    if documentation_to_add != fieldInfo.description:
                        fieldInfo.description = documentation_to_add
                        need_write = True
        else:
            # create a brand new editable dataset properties aspect
            current_editable_schema_metadata = EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[field_info_to_set], created=current_timestamp,
            )
            need_write = True

        if need_write:
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=current_editable_schema_metadata,
            )
            self.graph.emit(event)
            log.info(f"Documentation added to dataset {dataset_urn}")
        else:
            log.info("Documentation already exists and is identical, omitting write")

    def add_documentation_link(
        self, platform: str, name: str, link_to_add: str, link_description: str,
    ) -> None:
        """
        增加documentation
        :param platform:
        :param name:
        :param link_to_add:
        :param link_description:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)

        # Some helpful variables to fill out objects later
        now: int = int(time.time() * 1000)  # milliseconds since epoch
        current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")
        institutional_memory_element = InstitutionalMemoryMetadataClass(
            url=link_to_add,
            description=link_description,
            createStamp=current_timestamp,
        )

        current_institutional_memory = self.graph.get_aspect(
            entity_urn=dataset_urn, aspect_type=InstitutionalMemoryClass
        )

        need_write = False
        if current_institutional_memory:
            if link_to_add not in [
                x.url for x in current_institutional_memory.elements
            ]:
                current_institutional_memory.elements.append(
                    institutional_memory_element
                )
                need_write = True
        else:
            # create a brand new institutional memory aspect
            current_institutional_memory = InstitutionalMemoryClass(
                elements=[institutional_memory_element]
            )
            need_write = True

        if need_write:
            event = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=current_institutional_memory,
            )
            self.graph.emit(event)
            log.info(f"Link {link_to_add} added to dataset {dataset_urn}")

        else:
            log.info(
                f"Link {link_to_add} already exists and is identical, omitting write"
            )

    def add_documentation(
        self, platform: str, name: str, documentation_to_add: str,
    ) -> None:
        """
        增加documentation
        :param platform:
        :param name:
        :param documentation_to_add:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)

        # Some helpful variables to fill out objects later
        now = int(time.time() * 1000)  # milliseconds since epoch
        current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")
        current_editable_properties = self.graph.get_aspect(
            entity_urn=dataset_urn, aspect_type=EditableDatasetPropertiesClass
        )

        need_write = False
        if current_editable_properties:
            if documentation_to_add != current_editable_properties.description:
                current_editable_properties.description = documentation_to_add
                need_write = True
        else:
            # create a brand new editable dataset properties aspect
            current_editable_properties = EditableDatasetPropertiesClass(
                created=current_timestamp, description=documentation_to_add
            )
            need_write = True

        if need_write:
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=current_editable_properties,
            )
            self.graph.emit(event)
            log.info(f"Documentation added to dataset {dataset_urn}")

        else:
            log.info("Documentation already exists and is identical, omitting write")

    def read_schema(self, platform: str, name: str) -> Dict[str, Optional[Aspect]]:
        """
        获取schema
        :param platform:
        :param name:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)

        result = self.graph.get_aspects_for_entity(
            entity_urn=dataset_urn,
            aspects=["schemaMetadata"],
            aspect_types=[SchemaMetadataClass],
        )
        return result

    def read_properties(self, platform: str, name: str) -> Dict[str, Optional[Aspect]]:
        """
        获取properties
        :param platform:
        :param name:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)
        result = self.graph.get_aspects_for_entity(
            entity_urn=dataset_urn,
            aspects=["datasetProperties"],
            aspect_types=[DatasetPropertiesClass],
        )
        return result

    def batch_add_deprecation(
        self, dataset_list: List[Dict], deprecated: str = "true"
    ) -> Dict:
        """
        批量增加deprecation
        :param dataset_list:
        :param deprecated:
        :return:
        """
        resource_urn: List = [
            f"""{{resourceUrn:"{dataset_key_to_urn(key=DatasetKeyClass(platform=i['platform'], name=i['name'], origin=self.env))}"}} """
            for i in dataset_list
        ]

        query: str = f"""
        mutation batchUpdateDeprecation {{
            batchUpdateDeprecation(
              input: {{
                deprecated: {deprecated},
                resources: [{','.join(resource_urn)}]
              }}
            )
        }}
        """
        result = self.graph.execute_graphql(query=query)
        return result

    def add_deprecation(
        self, platform: str, name: str, deprecated: str = "true"
    ) -> Dict:
        """
        增加 deprecation
        :param platform:
        :param name:
        :param deprecated:
        :return:
        """
        query: str = f"""
        mutation updateDeprecation {{
            updateDeprecation(input: {{ urn: "{dataset_key_to_urn(key=DatasetKeyClass(platform=platform, name=name, origin=self.env))}", deprecated: {deprecated} }})
        }}
        """
        result = self.graph.execute_graphql(query=query)
        return result

    def read_deprecation(self, platform: str, name: str) -> Dict:
        """
        读取 deprecation
        :param platform:
        :param name:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)

        result = self.graph.get_aspects_for_entity(
            entity_urn=dataset_urn,
            aspects=["deprecation"],
            aspect_types=[DeprecationClass],
        )
        return result

    def remove_domain(self, platform: str, name: str) -> Dict:
        """
        移除domain
        :param platform:
        :param name:
        :return:
        """
        query: str = f"""
        mutation unsetDomain {{
            unsetDomain(
              entityUrn:"{dataset_key_to_urn(key=DatasetKeyClass(platform=platform, name=name, origin=self.env))}"
            )
        }}
        """
        result = self.graph.execute_graphql(query=query)
        return result

    def add_domain(self, platform: str, name: str, domain: str) -> Dict:
        """
        增加domain
        :param platform:
        :param name:
        :param domain:
        :return:
        """
        query: str = f"""
        mutation setDomain {{
            setDomain(domainUrn: "{make_domain_urn(domain=domain)}", entityUrn: "{make_dataset_urn(platform=platform, name=name)}")
        }}
        """
        result = self.graph.execute_graphql(query=query)
        return result

    def read_domain(self, platform: str, name: str) -> Dict[str, Optional[Aspect]]:
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)

        result = self.graph.get_aspects_for_entity(
            entity_urn=dataset_urn, aspects=["domains"], aspect_types=[DomainsClass],
        )
        return result

    def create_domain(self, domain: str, domain_name: str, domain_desc) -> None:
        """
        创建domain
        :param domain:
        :param domain_name:
        :param domain_desc:
        :return:
        """
        domain_urn = make_domain_urn(domain=domain)
        domain_properties_aspect = DomainPropertiesClass(
            name=domain_name, description=domain_desc
        )

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityType="domain",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=domain_urn,
            aspect=domain_properties_aspect,
        )

        self.emitter.emit(event)
        log.info(f"Created domain {domain_urn}")

    def remove_owners(self, owner_list: List, dataset_list: List[Dict]) -> Dict:
        """
        批量移除owner
        :param owner_list:
        :param dataset_list:
        :return:
        """
        owner_urn: List = [f""" "{make_user_urn(username=i)}" """ for i in owner_list]
        resource_urn: List = [
            f"""{{resourceUrn:"{make_dataset_urn(platform=i['platform'], name=i['name'])}"}} """
            for i in dataset_list
        ]
        query: str = f"""
        mutation batchRemoveOwners {{
            batchRemoveOwners(
              input: {{
                ownerUrns:[{','.join(owner_urn)}],
                resources: [{','.join(resource_urn)}]
              }}
            )
        }}
        """
        result = self.graph.execute_graphql(query=query)
        return result

    def add_owners(self, platform: str, name: str, owner: str) -> None:
        """
        增加owner
        :param platform:
        :param name:
        :param owner:
        :return:
        """
        owner_to_add = make_user_urn(owner)
        ownership_type = OwnershipTypeClass.TECHNICAL_OWNER
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)

        owner_class_to_add = OwnerClass(owner=owner_to_add, type=ownership_type)
        ownership_to_add = OwnershipClass(owners=[owner_class_to_add])

        current_owners: Optional[OwnershipClass] = self.graph.get_aspect(
            entity_urn=dataset_urn, aspect_type=OwnershipClass
        )

        need_write = False
        if current_owners:
            if (owner_to_add, ownership_type) not in [
                (x.owner, x.type) for x in current_owners.owners
            ]:
                # owners exist, but this owner is not present in the current owners
                current_owners.owners.append(owner_class_to_add)
                need_write = True
        else:
            # create a brand new ownership aspect
            current_owners = ownership_to_add
            need_write = True

        if need_write:
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=current_owners,
            )
            self.graph.emit(event)
            log.info(
                f"Owner {owner_to_add}, type {ownership_type} added to dataset {dataset_urn}"
            )
        else:
            log.info(f"Owner {owner_to_add} already exists, omitting write")

    def read_owners(self, platform: str, name: str) -> Dict[str, Optional[Aspect]]:
        """
        读取owners
        :param platform:
        :param name:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)

        result = self.graph.get_aspects_for_entity(
            entity_urn=dataset_urn,
            aspects=["ownership"],
            aspect_types=[OwnershipClass],
        )

        return result

    def upsert_group(self, group_info: Dict) -> None:
        """
        更新 group
        :param group_info:
        :return:
        """
        group = CorpGroup(
            id=group_info["id"],
            owners=[str(CorpuserUrn.create_from_id(i)) for i in group_info["owners"]],
            members=[
                str(CorpuserUrn.create_from_id("bar@acryl.io"))
                for i in group_info["members"]
            ],
            display_name=group_info["display_name"],
            email=group_info["email"],
            description=group_info["description"],
            slack=group_info["slack"],
        )

        for event in group.generate_mcp(
            generation_config=CorpGroupGenerationConfig(
                override_editable=False, datahub_graph=self.graph
            )
        ):
            self.graph.emit(event)
        log.info(f"Upserted group {group.urn}")

    def upsert_user(self, user_info: Dict) -> None:
        """
        更新user
        :param user_info:
        :return:
        """
        user: CorpUser = CorpUser(**user_info)

        for event in user.generate_mcp(
            generation_config=CorpUserGenerationConfig(override_editable=False)
        ):
            self.graph.emit(event)
        log.info(f"Upserted user {user.urn}")

    def remove_terms(self, terms_list: List, dataset_list: List[Dict]) -> Dict:
        """
        批量移除terms
        :param terms_list:
        :param dataset_list:
        :return:
        """
        terms_urn: List = [f""" "{make_term_urn(term=i)}" """ for i in terms_list]
        resource_urn: List = [
            f"""{{resourceUrn:"{make_dataset_urn(platform=i['platform'], name=i['name'])}"}} """
            for i in dataset_list
        ]

        query: str = f"""
        mutation batchRemoveTerms {{
            batchRemoveTerms(
              input: {{
                termUrns: [{','.join(terms_urn)}],
                resources: [
                  {','.join(resource_urn)}
                  ]
              }}
            )
        }}
        """
        result = self.graph.execute_graphql(query=query)

        return result

    def column_add_terms(
        self, platform: str, name: str, terms: str, column: str
    ) -> None:
        """
        增加terms至实体-列
        :param platform:
        :param name:
        :param terms:
        :param column:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)
        term_to_add = make_term_urn(terms)

        current_editable_schema_metadata = self.graph.get_aspect(
            entity_urn=dataset_urn, aspect_type=EditableSchemaMetadataClass
        )

        # Some pre-built objects to help all the conditional pathways
        now = int(time.time() * 1000)  # milliseconds since epoch
        current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")

        term_association_to_add = GlossaryTermAssociationClass(urn=term_to_add)
        term_aspect_to_set = GlossaryTermsClass(
            terms=[term_association_to_add], auditStamp=current_timestamp
        )
        field_info_to_set = EditableSchemaFieldInfoClass(
            fieldPath=column, glossaryTerms=term_aspect_to_set
        )

        need_write = False
        field_match = False
        if current_editable_schema_metadata:
            for fieldInfo in current_editable_schema_metadata.editableSchemaFieldInfo:
                if (
                    self.get_simple_field_path_from_v2_field_path(fieldInfo.fieldPath)
                    == column
                ):
                    # we have some editable schema metadata for this field
                    field_match = True
                    if fieldInfo.glossaryTerms:
                        if term_to_add not in [
                            x.urn for x in fieldInfo.glossaryTerms.terms
                        ]:
                            # this tag is not present
                            fieldInfo.glossaryTerms.terms.append(
                                term_association_to_add
                            )
                            need_write = True
                    else:
                        fieldInfo.glossaryTerms = term_aspect_to_set
                        need_write = True

            if not field_match:
                # this field isn't present in the editable schema metadata aspect, add it
                field_info = field_info_to_set
                current_editable_schema_metadata.editableSchemaFieldInfo.append(
                    field_info
                )
                need_write = True

        else:
            # create a brand new editable schema metadata aspect
            current_editable_schema_metadata = EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[field_info_to_set], created=current_timestamp,
            )
            need_write = True

        if need_write:
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=current_editable_schema_metadata,
            )
            self.graph.emit(event)
            log.info(
                f"Term {term_to_add} added to column {column} of dataset {dataset_urn}"
            )

        else:
            log.info(
                f"Term {term_to_add} already attached to column {column}, omitting write"
            )

    def add_terms(self, platform: str, name: str, terms: str) -> None:
        """
        增加terms至实体
        :param platform:
        :param name:
        :param terms:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)

        current_terms: Optional[GlossaryTermsClass] = self.graph.get_aspect(
            entity_urn=dataset_urn, aspect_type=GlossaryTermsClass
        )

        term_to_add = make_term_urn(terms)
        term_association_to_add = GlossaryTermAssociationClass(urn=term_to_add)
        # an audit stamp that basically says we have no idea when these terms were added to this dataset
        # change the time value to (time.time() * 1000) if you want to specify the current time of running this code as the time
        unknown_audit_stamp = AuditStampClass(time=0, actor="urn:li:corpuser:ingestion")
        need_write = False
        if current_terms:
            if term_to_add not in [x.urn for x in current_terms.terms]:
                # terms exist, but this term is not present in the current terms
                current_terms.terms.append(term_association_to_add)
                need_write = True
        else:
            # create a brand new terms aspect
            current_terms = GlossaryTermsClass(
                terms=[term_association_to_add], auditStamp=unknown_audit_stamp,
            )
            need_write = True

        if need_write:
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=current_terms,
            )
            self.graph.emit(event)
        else:
            log.info(f"Term {term_to_add} already exists, omitting write")

    def read_terms(self, platform: str, name: str) -> Dict[str, Optional[Aspect]]:
        """
        读取实体的terms
        :param platform:
        :param name:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name, env=self.env)

        result = self.graph.get_aspects_for_entity(
            entity_urn=dataset_urn,
            aspects=["glossaryTerms"],
            aspect_types=[GlossaryTermsClass],
        )
        return result

    def create_terms(self, terms_name: str, terms_desc: str) -> None:
        """
        创建terms
        :param terms_name:
        :param terms_desc:
        :return:
        """
        term_urn: str = make_term_urn(terms_name)
        term_properties_aspect = GlossaryTermInfoClass(
            definition=terms_desc, name=terms_name, termSource="",
        )

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=term_urn, aspect=term_properties_aspect,
        )
        self.emitter.emit(event)

    def column_remove_tags(
        self, platform: str, name: str, tag: str, column: str
    ) -> Dict:
        """
        移除实体-列的tag
        :param platform:
        :param name:
        :param tag:
        :param column:
        :return:
        """
        query: str = f"""
        mutation removeTag {{
            removeTag(
              input: {{
                tagUrn: "{make_tag_urn(tag=tag)}",
                resourceUrn: "{make_dataset_urn(platform=platform, name=name)}",
                subResourceType:DATASET_FIELD,
                subResource:"{column}" }})
        }}
        """
        result = self.graph.execute_graphql(query=query)

        return result

    def get_simple_field_path_from_v2_field_path(self, field_path: str) -> str:
        """A helper function to extract simple . path notation from the v2 field path"""
        if not field_path.startswith("[version=2.0]"):
            # not a v2, we assume this is a simple path
            return field_path
            # this is a v2 field path
        tokens = [
            t
            for t in field_path.split(".")
            if not (t.startswith("[") or t.endswith("]"))
        ]

        return ".".join(tokens)

    def column_add_tags(self, platform: str, name: str, tag: str, column: str) -> None:
        """
        增加tag至实体-列
        :param platform:
        :param name:
        :param tag:
        :param column:
        :return:
        """

        dataset_urn: str = make_dataset_urn(platform=platform, name=name)
        tag_to_add = make_tag_urn(tag)

        current_editable_schema_metadata = self.graph.get_aspect(
            entity_urn=dataset_urn, aspect_type=EditableSchemaMetadataClass,
        )

        # Some pre-built objects to help all the conditional pathways
        tag_association_to_add = TagAssociationClass(tag=tag_to_add)
        tags_aspect_to_set = GlobalTagsClass(tags=[tag_association_to_add])
        field_info_to_set = EditableSchemaFieldInfoClass(
            fieldPath=column, globalTags=tags_aspect_to_set
        )

        need_write = False
        field_match = False
        if current_editable_schema_metadata:
            for fieldInfo in current_editable_schema_metadata.editableSchemaFieldInfo:
                if (
                    self.get_simple_field_path_from_v2_field_path(fieldInfo.fieldPath)
                    == column
                ):
                    # we have some editable schema metadata for this field
                    field_match = True
                    if fieldInfo.globalTags:
                        if tag_to_add not in [x.tag for x in fieldInfo.globalTags.tags]:
                            # this tag is not present
                            fieldInfo.globalTags.tags.append(tag_association_to_add)
                            need_write = True
                    else:
                        fieldInfo.globalTags = tags_aspect_to_set
                        need_write = True

            if not field_match:
                # this field isn't present in the editable schema metadata aspect, add it
                field_info = field_info_to_set
                current_editable_schema_metadata.editableSchemaFieldInfo.append(
                    field_info
                )
                need_write = True

        else:
            # create a brand new editable schema metadata aspect
            now = int(time.time() * 1000)  # milliseconds since epoch
            current_timestamp = AuditStampClass(
                time=now, actor="urn:li:corpuser:ingestion"
            )
            current_editable_schema_metadata = EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[field_info_to_set], created=current_timestamp,
            )
            need_write = True

        if need_write:
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=current_editable_schema_metadata,
            )
            self.graph.emit(event)
            log.info(
                f"Tag {tag_to_add} added to column {column} of dataset {dataset_urn}"
            )
        else:
            log.info(
                f"Tag {tag_to_add} already attached to column {column}, omitting write"
            )

    def add_tags(self, platform: str, name: str, tag: str) -> None:
        """
        增加tag至实体
        :param platform:
        :param name:
        :param tag:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name)

        current_tags: Optional[GlobalTagsClass] = self.graph.get_aspect(
            entity_urn=dataset_urn, aspect_type=GlobalTagsClass,
        )

        tag_to_add = make_tag_urn(tag)
        tag_association_to_add = TagAssociationClass(tag=tag_to_add)

        need_write = False
        if current_tags:
            if tag_to_add not in [x.tag for x in current_tags.tags]:
                # tags exist, but this tag is not present in the current tags
                current_tags.tags.append(TagAssociationClass(tag_to_add))
                need_write = True
        else:
            # create a brand new tags aspect
            current_tags = GlobalTagsClass(tags=[tag_association_to_add])
            need_write = True

        if need_write:
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=current_tags,
            )
            self.graph.emit(event)
            log.info(f"Tag {tag_to_add} added to dataset {dataset_urn}")
        else:
            log.info(f"Tag {tag_to_add} already exists, omitting write")

    def read_tags(self, platform: str, name: str) -> Dict[str, Optional[Aspect]]:
        """
        实体的tags
        :param platform:
        :param name:
        :return:
        """
        dataset_urn: str = make_dataset_urn(platform=platform, name=name)
        result = self.graph.get_aspects_for_entity(
            entity_urn=dataset_urn,
            aspects=["globalTags"],
            aspect_types=[GlobalTagsClass],
        )
        return result

    def create_tags(self, tag_name: str, tag_desc: str = None) -> None:
        """
        创建tag
        :param tag_name:
        :param tag_desc:
        :return:
        """
        tag_urn = make_tag_urn(tag_name)
        tag_properties_aspect = TagPropertiesClass(name=tag_name, description=tag_desc,)

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=tag_urn, aspect=tag_properties_aspect,
        )
        self.emitter.emit(event)

    def make_lineage_column(self, lineage_info: List, down: Dict) -> None:
        """
        列血缘
        :param lineage_info:
        :param down:
        :return:
        """

        def datasetUrn(platform, tbl):
            return make_dataset_urn(platform=platform, name=tbl)

        def fldUrn(platfrom, tbl, fld):
            return make_schema_field_urn(
                parent_urn=datasetUrn(platform=platfrom, tbl=tbl), field_path=fld
            )

        fineGrainedLineages = [
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=[
                    fldUrn(platfrom=j["platform"], tbl=j["name"], fld=j["column"])
                    for j in i["ups"]
                ],
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[
                    fldUrn(
                        platfrom=down["platform"],
                        tbl=down["name"],
                        fld=i["down"]["column"],
                    )
                ],
            )
            for i in lineage_info
        ]

        # this is just to check if any conflicts with existing Upstream, particularly the DownstreamOf relationship
        up_list = []
        for i in lineage_info:
            up_list.extend(i["ups"])

        upstream = [
            Upstream(
                dataset=datasetUrn(platform=i["platform"], tbl=i["name"]),
                type=DatasetLineageType.TRANSFORMED,
            )
            for i in up_list
        ]

        fieldLineages = UpstreamLineage(
            upstreams=upstream, fineGrainedLineages=fineGrainedLineages
        )

        lineage_mcp = MetadataChangeProposalWrapper(
            entityUrn=datasetUrn(platform=down["platform"], tbl=down["name"]),
            aspect=fieldLineages,
        )

        self.emitter.emit_mcp(lineage_mcp)

    def make_lineage(self, lineage_info: Dict) -> None:
        """
        表血缘
        :return:
        """

        lineage_mce = make_lineage_mce(
            upstream_urns=[
                make_dataset_urn(i["platform"], i["name"]) for i in lineage_info["ups"]
            ],
            downstream_urn=make_dataset_urn(
                platform=lineage_info["down"]["platform"],
                name=lineage_info["down"]["name"],
            ),
        )

        self.emitter.emit_mce(lineage_mce)
