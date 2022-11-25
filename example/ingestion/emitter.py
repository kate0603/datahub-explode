# -*- coding: utf-8 -*-
"""
    Created by w at 2022/11/18.
    Description:
    Changelog: all notable changes to this file will be documented
"""
from typing import Iterable
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.api.source import Source
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from example.config import datahub_server


class DatahubSink(object):
    def __init__(self, source: Source):
        self.server: str = datahub_server["server"]
        self.token: str = datahub_server["token"]
        self.source = source

    def post(self):
        """
        :return:
        """
        wus: Iterable = self.source.get_workunits()
        emitter = DatahubRestEmitter(
            self.server, token=self.token, disable_ssl_verification=True
        )
        for wu in wus:
            # if isinstance(wu.metadata, MetadataChangeEvent):
            #     emitter.emit_mce(wu.metadata)
            # elif isinstance(
            #     wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            # ):
            #     # emitter.emit_mcp(wu.metadata)
            # else:
            print(wu.metadata.__class__)
            print(wu.metadata)
