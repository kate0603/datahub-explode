# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/17.
    Description:
    Changelog: all notable changes to this file will be documented
"""
import json
from typing import Dict, List
import requests
from example.config import datahub_server
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_term_urn,
    make_domain_urn,
    make_tag_urn,
    make_dataset_urn,
    make_data_flow_urn,
    make_data_job_urn,
)


class DataHubQuery(object):
    def __init__(self):
        self.flow_id = "1c92020f-0180-1000-cb79-26e7eb563c40"
        self.env = "DEV"
        self.urn_nifi = make_data_flow_urn("nifi", self.flow_id, self.env)
        self.urn_datajob = make_data_job_urn("nifi", self.flow_id, "facebook", self.env)

    def request_graphql(cls, query) -> Dict:
        """
        通用请求
        :param query:
        :return:
        """
        headers = {
            "Authorization": f"Bearer {datahub_server['token']}",
            "X-RestLi-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
            "X-DataHub-Actor": "urn:li:corpuser:datahub",
        }

        res = requests.post(
            datahub_server["api"], data=json.dumps({"query": query}), headers=headers
        )
        return res.json()

    def get_datajob_list(self) -> List:
        """
        nifi-dataflow-datajob-list
        :return:[{
        "entity": {
            "urn": "urn:li:dataJob:(urn:li:dataFlow:(nifi,1c92020f-0180-1000-cb79-26e7eb563c40,DEV),tiktok)",
            "type": "DATA_JOB",
            "jobId": "tiktok",
            "dataFlow": {
                "urn": "urn:li:dataFlow:(nifi,1c92020f-0180-1000-cb79-26e7eb563c40,DEV)",
                "type": "DATA_FLOW",
                "orchestrator": "nifi",
                "platform": {
                    "urn": "urn:li:dataPlatform:nifi",
                    "type": "DATA_PLATFORM",
                    "name": "nifi",
                    "properties": {
                        "type": "OTHERS",
                        "displayName": "Nifi",
                        "datasetNameDelimiter": ".",
                        "logoUrl": "/assets/platforms/nifilogo.svg",
                        "__typename": "DataPlatformProperties",
                    },
                    "displayName": None,
                    "info": None,
                    "__typename": "DataPlatform",
                },
                "__typename": "DataFlow",
            },
            "ownership": None,
            "properties": {
                "name": "tiktok",
                "description": "属性说明\n1. platform：任务名称，用于在消息通知时区分不同的任务\n2. start_date：采集开始日期，literal()表达式中填写日期字符串，如果为空将替代为默认日期\n3. end_date：采集结束日期",
                "__typename": "DataJobProperties",
            },
            "editableProperties": None,
            "globalTags": None,
            "glossaryTerms": None,
            "deprecation": None,
            "__typename": "DataJob",
        },
        "__typename": "EntityRelationship",
    },]
        """
        query = (
            """{
        dataFlow(urn: "%s") {
            ...dataFlowFields
            upstream: lineage(input: {direction: UPSTREAM, start: 0, count: 100}) {
              ...partialLineageResults
              __typename
            }
            downstream: lineage(input: {direction: DOWNSTREAM, start: 0, count: 100}) {
              ...partialLineageResults
              __typename
            }
            childJobs: relationships(
              input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 100}
            ) {
              start
              count
              total
              relationships {
                entity {
                  ... on DataJob {
                    urn
                    type
                    jobId
                    dataFlow {
                      urn
                      type
                      orchestrator
                      platform {
                        ...platformFields
                        __typename
                      }
                      __typename
                    }
                    ownership {
                      ...ownershipFields
                      __typename
                    }
                    properties {
                      name
                      description
                      __typename
                    }
                    editableProperties {
                      description
                      __typename
                    }
                    globalTags {
                      ...globalTagsFields
                      __typename
                    }
                    glossaryTerms {
                      ...glossaryTerms
                      __typename
                    }
                    deprecation {
                      ...deprecationFields
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        
        fragment dataFlowFields on DataFlow {
          urn
          type
          orchestrator
          flowId
          cluster
          platform {
            ...platformFields
            __typename
          }
          properties {
            name
            description
            project
            externalUrl
            customProperties {
              key
              value
              __typename
            }
            __typename
          }
          editableProperties {
            description
            __typename
          }
          ownership {
            ...ownershipFields
            __typename
          }
          globalTags {
            ...globalTagsFields
            __typename
          }
          institutionalMemory {
            ...institutionalMemoryFields
            __typename
          }
          glossaryTerms {
            ...glossaryTerms
            __typename
          }
          domain {
            ...entityDomain
            __typename
          }
          status {
            removed
            __typename
          }
          deprecation {
            ...deprecationFields
            __typename
          }
          dataPlatformInstance {
            ...dataPlatformInstanceFields
            __typename
          }
          __typename
        }
        
        fragment platformFields on DataPlatform {
          urn
          type
          name
          properties {
            type
            displayName
            datasetNameDelimiter
            logoUrl
            __typename
          }
          displayName
          info {
            type
            displayName
            datasetNameDelimiter
            logoUrl
            __typename
          }
          __typename
        }
        
        fragment ownershipFields on Ownership {
          owners {
            owner {
              ... on CorpUser {
                urn
                type
                username
                info {
                  active
                  displayName
                  title
                  email
                  firstName
                  lastName
                  fullName
                  __typename
                }
                properties {
                  active
                  displayName
                  title
                  email
                  firstName
                  lastName
                  fullName
                  __typename
                }
                editableProperties {
                  displayName
                  title
                  pictureLink
                  email
                  __typename
                }
                __typename
              }
              ... on CorpGroup {
                urn
                type
                name
                properties {
                  displayName
                  email
                  __typename
                }
                info {
                  displayName
                  email
                  admins {
                    urn
                    username
                    info {
                      active
                      displayName
                      title
                      email
                      firstName
                      lastName
                      fullName
                      __typename
                    }
                    editableInfo {
                      pictureLink
                      teams
                      skills
                      __typename
                    }
                    __typename
                  }
                  members {
                    urn
                    username
                    info {
                      active
                      displayName
                      title
                      email
                      firstName
                      lastName
                      fullName
                      __typename
                    }
                    editableInfo {
                      pictureLink
                      teams
                      skills
                      __typename
                    }
                    __typename
                  }
                  groups
                  __typename
                }
                __typename
              }
              __typename
            }
            type
            associatedUrn
            __typename
          }
          lastModified {
            time
            __typename
          }
          __typename
        }
        
        fragment globalTagsFields on GlobalTags {
          tags {
            tag {
              urn
              name
              description
              properties {
                colorHex
                __typename
              }
              __typename
            }
            associatedUrn
            __typename
          }
          __typename
        }
        
        fragment institutionalMemoryFields on InstitutionalMemory {
          elements {
            url
            author {
              urn
              username
              __typename
            }
            description
            created {
              actor
              time
              __typename
            }
            __typename
          }
          __typename
        }
        
        fragment glossaryTerms on GlossaryTerms {
          terms {
            term {
              ...glossaryTerm
              __typename
            }
            associatedUrn
            __typename
          }
          __typename
        }
        
        fragment glossaryTerm on GlossaryTerm {
          urn
          name
          type
          hierarchicalName
          properties {
            name
            definition
            termSource
            __typename
          }
          __typename
        }
        
        fragment entityDomain on DomainAssociation {
          domain {
            urn
            properties {
              name
              __typename
            }
            __typename
          }
          associatedUrn
          __typename
        }
        
        fragment deprecationFields on Deprecation {
          actor
          deprecated
          note
          decommissionTime
          __typename
        }
        
        fragment dataPlatformInstanceFields on DataPlatformInstance {
          urn
          type
          platform {
            ...platformFields
            __typename
          }
          instanceId
          __typename
        }
        
        fragment partialLineageResults on EntityLineageResult {
          start
          count
          total
          __typename
        }
            """
            % self.urn_nifi
        )
        dataflow = self.request_graphql(query)["data"]["dataFlow"]
        child_jobs = dataflow["childJobs"]
        datajob_list = [i for i in child_jobs["relationships"]]
        return datajob_list

    def get_datajob_runs(self) -> List:
        """
        nifi-datajob-task-runs
        :return:{
        "urn": "urn:li:dataProcessInstance:00757c6e-649e-48dc-82ee-820dc0f7e79d",
        "type": "DATA_PROCESS_INSTANCE",
        "created": {
            "time": 1668044146688,
            "actor": "urn:li:corpuser:datahub",
            "__typename": "AuditStamp",
        },
        "name": "facebook-2022-11-10 09:23:47",
        "state": [
            {
                "status": "COMPLETE",
                "attempt": None,
                "result": {
                    "resultType": "FAILURE",
                    "nativeResultType": "nifi",
                    "__typename": "DataProcessInstanceRunResult",
                },
                "timestampMillis": 1668044146651,
                "__typename": "DataProcessRunEvent",
            }
        ],
        "inputs": {
            "start": 0,
            "count": 0,
            "total": 0,
            "relationships": [],
            "__typename": "EntityRelationshipsResult",
        },
        "outputs": {
            "start": 0,
            "count": 0,
            "total": 0,
            "relationships": [],
            "__typename": "EntityRelationshipsResult",
        },
        "parentTemplate": {
            "start": 0,
            "count": 1,
            "total": 1,
            "relationships": [
                {
                    "type": "InstanceOf",
                    "direction": "OUTGOING",
                    "entity": {
                        "urn": "urn:li:dataJob:(urn:li:dataFlow:(nifi,1c92020f-0180-1000-cb79-26e7eb563c40,DEV),facebook)",
                        "type": "DATA_JOB",
                        "dataFlow": {
                            "urn": "urn:li:dataFlow:(nifi,1c92020f-0180-1000-cb79-26e7eb563c40,DEV)",
                            "type": "DATA_FLOW",
                            "orchestrator": "nifi",
                            "flowId": "1c92020f-0180-1000-cb79-26e7eb563c40",
                            "cluster": "DEV",
                            "properties": {
                                "name": "NiFi Flow",
                                "description": None,
                                "project": None,
                                "externalUrl": "https://10.20.110.130:31487/nifi/?processGroupId=1c92020f-0180-1000-cb79-26e7eb563c40&componentIds=",
                                "customProperties": [
                                    {
                                        "key": "version",
                                        "value": "1.16.0",
                                        "__typename": "CustomPropertiesEntry",
                                    },
                                    {
                                        "key": "clustered",
                                        "value": "False",
                                        "__typename": "CustomPropertiesEntry",
                                    },
                                ],
                                "__typename": "DataFlowProperties",
                            },
                            "editableProperties": None,
                            "ownership": None,
                            "platform": {
                                "urn": "urn:li:dataPlatform:nifi",
                                "type": "DATA_PLATFORM",
                                "lastIngested": None,
                                "name": "nifi",
                                "properties": {
                                    "type": "OTHERS",
                                    "displayName": "Nifi",
                                    "datasetNameDelimiter": ".",
                                    "logoUrl": "/assets/platforms/nifilogo.svg",
                                    "__typename": "DataPlatformProperties",
                                },
                                "displayName": None,
                                "info": None,
                                "__typename": "DataPlatform",
                            },
                            "domain": None,
                            "deprecation": None,
                            "__typename": "DataFlow",
                        },
                        "jobId": "facebook",
                        "properties": {
                            "name": "facebook",
                            "description": "属性说明\n1. platform：任务名称，用于在消息通知时区分不同的任务\n2. start_date：采集开始日期，literal()表达式中填写日期字符串，如果为空将替代为默认日期\n3. end_date：采集结束日期",
                            "externalUrl": "https://10.20.110.130:31487/nifi/?processGroupId=6eda37ca-6343-3276-2605-a10664171d2e&componentIds=08c7e47c-5306-3ed0-c7c3-fe1a114bce31",
                            "customProperties": [
                                {
                                    "key": "executionNode",
                                    "value": "ALL",
                                    "__typename": "CustomPropertiesEntry",
                                },
                                {
                                    "key": "schedulingPeriod",
                                    "value": "0 30 9 1/1 * ? *",
                                    "__typename": "CustomPropertiesEntry",
                                },
                                {
                                    "key": "concurrentlySchedulableTaskCount",
                                    "value": "1",
                                    "__typename": "CustomPropertiesEntry",
                                },
                                {
                                    "key": "schedulingStrategy",
                                    "value": "CRON_DRIVEN",
                                    "__typename": "CustomPropertiesEntry",
                                },
                                {
                                    "key": "properties",
                                    "value": '{"File Size": "1B", "Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "false", "generate-ff-custom-text": null, "character-set": "UTF-8", "mime-type": null, "end_date": "${literal(\'\'):replaceEmpty(${now():toNumber():minus(86400000):format(\'yyyy-MM-dd\')})}", "platform": "facebook", "start_date": "${literal(\'\'):replaceEmpty(${now():toNumber():minus(2505600000):format(\'yyyy-MM-dd\')})}"}',
                                    "__typename": "CustomPropertiesEntry",
                                },
                            ],
                            "__typename": "DataJobProperties",
                        },
                        "deprecation": None,
                        "dataPlatformInstance": None,
                        "editableProperties": None,
                        "status": None,
                        "__typename": "DataJob",
                    },
                    "__typename": "EntityRelationship",
                }
            ],
            "__typename": "EntityRelationshipsResult",
        },
        "externalUrl": None,
        "__typename": "DataProcessInstance",
    },
        """
        start = 0
        count = 10
        query: str = """{
                  dataJob(urn: "%s") {
                    runs(start: %d, count: %d) {
                      ...runResults
                      __typename
                    }
                    __typename
                  }
                }
    
                fragment runResults on DataProcessInstanceResult {
                  count
                  start
                  total
                  runs {
                    urn
                    type
                    created {
                      time
                      actor
                      __typename
                    }
                    name
                    state(startTimeMillis: null, endTimeMillis: null, limit: 1) {
                      status
                      attempt
                      result {
                        resultType
                        nativeResultType
                        __typename
                      }
                      timestampMillis
                      __typename
                    }
                    inputs: relationships(
                      input: {types: ["Consumes"], direction: OUTGOING, start: 0, count: 20}
                    ) {
                      ...runRelationshipResults
                      __typename
                    }
                    outputs: relationships(
                      input: {types: ["Produces"], direction: OUTGOING, start: 0, count: 20}
                    ) {
                      ...runRelationshipResults
                      __typename
                    }
                    parentTemplate: relationships(
                      input: {types: ["InstanceOf"], direction: OUTGOING, start: 0, count: 1}
                    ) {
                      ...runRelationshipResults
                      __typename
                    }
                    externalUrl
                    __typename
                  }
                  __typename
                }
    
                fragment runRelationshipResults on EntityRelationshipsResult {
                  start
                  count
                  total
                  relationships {
                    type
                    direction
                    entity {
                      urn
                      type
                      ... on Dataset {
                        name
                        properties {
                          name
                          description
                          qualifiedName
                          __typename
                        }
                        editableProperties {
                          description
                          __typename
                        }
                        platform {
                          ...platformFields
                          __typename
                        }
                        subTypes {
                          typeNames
                          __typename
                        }
                        status {
                          removed
                          __typename
                        }
                        __typename
                      }
                      ... on DataJob {
                        urn
                        type
                        dataFlow {
                          ...nonRecursiveDataFlowFields
                          __typename
                        }
                        jobId
                        properties {
                          name
                          description
                          externalUrl
                          customProperties {
                            key
                            value
                            __typename
                          }
                          __typename
                        }
                        deprecation {
                          ...deprecationFields
                          __typename
                        }
                        dataPlatformInstance {
                          ...dataPlatformInstanceFields
                          __typename
                        }
                        editableProperties {
                          description
                          __typename
                        }
                        status {
                          removed
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
    
                fragment platformFields on DataPlatform {
                  urn
                  type
                  lastIngested
                  name
                  properties {
                    type
                    displayName
                    datasetNameDelimiter
                    logoUrl
                    __typename
                  }
                  displayName
                  info {
                    type
                    displayName
                    datasetNameDelimiter
                    logoUrl
                    __typename
                  }
                  __typename
                }
    
                fragment nonRecursiveDataFlowFields on DataFlow {
                  urn
                  type
                  orchestrator
                  flowId
                  cluster
                  properties {
                    name
                    description
                    project
                    externalUrl
                    customProperties {
                      key
                      value
                      __typename
                    }
                    __typename
                  }
                  editableProperties {
                    description
                    __typename
                  }
                  ownership {
                    ...ownershipFields
                    __typename
                  }
                  platform {
                    ...platformFields
                    __typename
                  }
                  domain {
                    ...entityDomain
                    __typename
                  }
                  deprecation {
                    ...deprecationFields
                    __typename
                  }
                  __typename
                }
    
                fragment ownershipFields on Ownership {
                  owners {
                    owner {
                      ... on CorpUser {
                        urn
                        type
                        username
                        info {
                          active
                          displayName
                          title
                          email
                          firstName
                          lastName
                          fullName
                          __typename
                        }
                        properties {
                          active
                          displayName
                          title
                          email
                          firstName
                          lastName
                          fullName
                          __typename
                        }
                        editableProperties {
                          displayName
                          title
                          pictureLink
                          email
                          __typename
                        }
                        __typename
                      }
                      ... on CorpGroup {
                        urn
                        type
                        name
                        properties {
                          displayName
                          email
                          __typename
                        }
                        info {
                          displayName
                          email
                          admins {
                            urn
                            username
                            info {
                              active
                              displayName
                              title
                              email
                              firstName
                              lastName
                              fullName
                              __typename
                            }
                            editableInfo {
                              pictureLink
                              teams
                              skills
                              __typename
                            }
                            __typename
                          }
                          members {
                            urn
                            username
                            info {
                              active
                              displayName
                              title
                              email
                              firstName
                              lastName
                              fullName
                              __typename
                            }
                            editableInfo {
                              pictureLink
                              teams
                              skills
                              __typename
                            }
                            __typename
                          }
                          groups
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    type
                    associatedUrn
                    __typename
                  }
                  lastModified {
                    time
                    __typename
                  }
                  __typename
                }
    
                fragment entityDomain on DomainAssociation {
                  domain {
                    urn
                    properties {
                      name
                      __typename
                    }
                    __typename
                  }
                  associatedUrn
                  __typename
                }
    
                fragment deprecationFields on Deprecation {
                  actor
                  deprecated
                  note
                  decommissionTime
                  __typename
                }
    
                fragment dataPlatformInstanceFields on DataPlatformInstance {
                  urn
                  type
                  platform {
                    ...platformFields
                    __typename
                  }
                  instanceId
                  __typename
                }
                """ % (
            self.urn_datajob,
            start,
            count,
        )
        data: List = self.request_graphql(query)["data"]["dataJob"]["runs"]["runs"]
        return data

    def get_browse_list(
        self,
        data_type: str = "DATA_FLOW",
        path: list = None,
        start: int = 0,
        count: int = 10,
    ) -> Dict:
        """
        nifi-browse
        :return:
        """
        path = path or ["nifi", self.env.lower()]
        query: str = """
            {
              browse(input: {
              type: %s,
              path: [
                "%s",
                "%s"
              ],
              start: %s,
              count: %s,
            }
              ) {
                entities {
                  urn
                  type
                  ... on Dataset {
                    name
                    origin
                    properties {
                      name
                      description
                      __typename
                    }
                    editableProperties {
                      description
                      __typename
                    }
                    platform {
                      ...platformFields
                      __typename
                    }
                    ownership {
                      ...ownershipFields
                      __typename
                    }
                    globalTags {
                      ...globalTagsFields
                      __typename
                    }
                    glossaryTerms {
                      ...glossaryTerms
                      __typename
                    }
                    subTypes {
                      typeNames
                      __typename
                    }
                    domain {
                      ...entityDomain
                      __typename
                    }
                    __typename
                  }
                  ... on Dashboard {
                    urn
                    type
                    tool
                    dashboardId
                    properties {
                      name
                      description
                      externalUrl
                      access
                      lastModified {
                        time
                        __typename
                      }
                      __typename
                    }
                    editableProperties {
                      description
                      __typename
                    }
                    ownership {
                      ...ownershipFields
                      __typename
                    }
                    globalTags {
                      ...globalTagsFields
                      __typename
                    }
                    glossaryTerms {
                      ...glossaryTerms
                      __typename
                    }
                    platform {
                      ...platformFields
                      __typename
                    }
                    domain {
                      ...entityDomain
                      __typename
                    }
                    subTypes {
                      typeNames
                      __typename
                    }
                    __typename
                  }
                  ... on GlossaryTerm {
                    ownership {
                      ...ownershipFields
                      __typename
                    }
                    properties {
                      name
                      description
                      definition
                      termSource
                      sourceRef
                      sourceUrl
                      customProperties {
                        key
                        value
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  ... on Chart {
                    urn
                    type
                    tool
                    chartId
                    properties {
                      name
                      description
                      externalUrl
                      type
                      access
                      lastModified {
                        time
                        __typename
                      }
                      __typename
                    }
                    editableProperties {
                      description
                      __typename
                    }
                    ownership {
                      ...ownershipFields
                      __typename
                    }
                    globalTags {
                      ...globalTagsFields
                      __typename
                    }
                    glossaryTerms {
                      ...glossaryTerms
                      __typename
                    }
                    platform {
                      ...platformFields
                      __typename
                    }
                    domain {
                      ...entityDomain
                      __typename
                    }
                    __typename
                  }
                  ... on DataFlow {
                    urn
                    type
                    orchestrator
                    flowId
                    cluster
                    properties {
                      name
                      description
                      project
                      __typename
                    }
                    editableProperties {
                      description
                      __typename
                    }
                    ownership {
                      ...ownershipFields
                      __typename
                    }
                    globalTags {
                      ...globalTagsFields
                      __typename
                    }
                    glossaryTerms {
                      ...glossaryTerms
                      __typename
                    }
                    platform {
                      ...platformFields
                      __typename
                    }
                    domain {
                      ...entityDomain
                      __typename
                    }
                    __typename
                  }
                  ... on DataJob {
                    urn
                    type
                    dataFlow {
                      ...nonRecursiveDataFlowFields
                      __typename
                    }
                    jobId
                    ownership {
                      ...ownershipFields
                      __typename
                    }
                    properties {
                      name
                      description
                      __typename
                    }
                    globalTags {
                      ...globalTagsFields
                      __typename
                    }
                    glossaryTerms {
                      ...glossaryTerms
                      __typename
                    }
                    editableProperties {
                      description
                      __typename
                    }
                    domain {
                      ...entityDomain
                      __typename
                    }
                    __typename
                  }
                  ... on MLFeatureTable {
                    urn
                    type
                    name
                    description
                    featureTableProperties {
                      description
                      mlFeatures {
                        urn
                        __typename
                      }
                      mlPrimaryKeys {
                        urn
                        __typename
                      }
                      __typename
                    }
                    ownership {
                      ...ownershipFields
                      __typename
                    }
                    platform {
                      ...platformFields
                      __typename
                    }
                    __typename
                  }
                  ... on MLModel {
                    name
                    origin
                    description
                    ownership {
                      ...ownershipFields
                      __typename
                    }
                    globalTags {
                      ...globalTagsFields
                      __typename
                    }
                    platform {
                      ...platformFields
                      __typename
                    }
                    __typename
                  }
                  ... on MLModelGroup {
                    name
                    origin
                    description
                    ownership {
                      ...ownershipFields
                      __typename
                    }
                    platform {
                      ...platformFields
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                groups {
                  name
                  count
                  __typename
                }
                start
                count
                total
                metadata {
                  path
                  totalNumEntities
                  __typename
                }
                __typename
              }
            }

            fragment platformFields on DataPlatform {
              urn
              type
              lastIngested
              name
              properties {
                type
                displayName
                datasetNameDelimiter
                logoUrl
                __typename
              }
              displayName
              info {
                type
                displayName
                datasetNameDelimiter
                logoUrl
                __typename
              }
              __typename
            }

            fragment ownershipFields on Ownership {
              owners {
                owner {
                  ... on CorpUser {
                    urn
                    type
                    username
                    info {
                      active
                      displayName
                      title
                      email
                      firstName
                      lastName
                      fullName
                      __typename
                    }
                    properties {
                      active
                      displayName
                      title
                      email
                      firstName
                      lastName
                      fullName
                      __typename
                    }
                    editableProperties {
                      displayName
                      title
                      pictureLink
                      email
                      __typename
                    }
                    __typename
                  }
                  ... on CorpGroup {
                    urn
                    type
                    name
                    properties {
                      displayName
                      email
                      __typename
                    }
                    info {
                      displayName
                      email
                      admins {
                        urn
                        username
                        info {
                          active
                          displayName
                          title
                          email
                          firstName
                          lastName
                          fullName
                          __typename
                        }
                        editableInfo {
                          pictureLink
                          teams
                          skills
                          __typename
                        }
                        __typename
                      }
                      members {
                        urn
                        username
                        info {
                          active
                          displayName
                          title
                          email
                          firstName
                          lastName
                          fullName
                          __typename
                        }
                        editableInfo {
                          pictureLink
                          teams
                          skills
                          __typename
                        }
                        __typename
                      }
                      groups
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                type
                associatedUrn
                __typename
              }
              lastModified {
                time
                __typename
              }
              __typename
            }

            fragment globalTagsFields on GlobalTags {
              tags {
                tag {
                  urn
                  type
                  name
                  description
                  properties {
                    colorHex
                    __typename
                  }
                  __typename
                }
                associatedUrn
                __typename
              }
              __typename
            }

            fragment glossaryTerms on GlossaryTerms {
              terms {
                term {
                  ...glossaryTerm
                  __typename
                }
                associatedUrn
                __typename
              }
              __typename
            }

            fragment glossaryTerm on GlossaryTerm {
              urn
              name
              type
              hierarchicalName
              properties {
                name
                description
                definition
                termSource
                customProperties {
                  key
                  value
                  __typename
                }
                __typename
              }
              ownership {
                ...ownershipFields
                __typename
              }
              parentNodes {
                ...parentNodesFields
                __typename
              }
              __typename
            }

            fragment parentNodesFields on ParentNodesResult {
              count
              nodes {
                ...glossaryNode
                __typename
              }
              __typename
            }

            fragment glossaryNode on GlossaryNode {
              urn
              type
              properties {
                name
                __typename
              }
              children: relationships(
                input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 10000}
              ) {
                total
                __typename
              }
              __typename
            }

            fragment entityDomain on DomainAssociation {
              domain {
                urn
                type
                properties {
                  name
                  description
                  __typename
                }
                __typename
              }
              associatedUrn
              __typename
            }

            fragment nonRecursiveDataFlowFields on DataFlow {
              urn
              type
              orchestrator
              flowId
              cluster
              properties {
                name
                description
                project
                externalUrl
                customProperties {
                  key
                  value
                  __typename
                }
                __typename
              }
              editableProperties {
                description
                __typename
              }
              ownership {
                ...ownershipFields
                __typename
              }
              platform {
                ...platformFields
                __typename
              }
              domain {
                ...entityDomain
                __typename
              }
              deprecation {
                ...deprecationFields
                __typename
              }
              __typename
            }

            fragment deprecationFields on Deprecation {
              actor
              deprecated
              note
              decommissionTime
              __typename
            }
            """ % (
            data_type,
            path[0],
            path[1],
            start,
            count,
        )
        data: Dict = self.request_graphql(query)["data"]["browse"]
        return data

    def get_root_glossary_terms(self) -> List:
        """
        glossaryTerms
        :return:[{
        "urn": "urn:li:glossaryTerm:65d36ce8-d338-4e51-8f52-eac899abce9b",
        "type": "GLOSSARY_TERM",
        "name": "65d36ce8-d338-4e51-8f52-eac899abce9b",
        "hierarchicalName": "65d36ce8-d338-4e51-8f52-eac899abce9b",
        "properties": {"name": "ncore", "__typename": "GlossaryTermProperties"},
        "__typename": "GlossaryTerm",
    },]
        """
        query: str = """query getRootGlossaryTerms {
              getRootGlossaryTerms(input: {start: 0, count: 1000}) {
                count
                start
                total
                terms {
                  ...childGlossaryTerm
                  __typename
                }
                __typename
              }
            }

            fragment childGlossaryTerm on GlossaryTerm {
              urn
              type
              name
              hierarchicalName
              properties {
                name
                __typename
              }
              __typename
            }
            """
        data: List = self.request_graphql(query)["data"]["getRootGlossaryTerms"][
            "terms"
        ]
        return data
