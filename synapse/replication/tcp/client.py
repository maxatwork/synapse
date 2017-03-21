# -*- coding: utf-8 -*-
# Copyright 2017 Vector Creations Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from twisted.internet import reactor
from twisted.internet.protocol import ReconnectingClientFactory

from .commands import FederationAckCommand
from .protocol import ClientReplicationStreamProtocol

import logging

logger = logging.getLogger(__name__)


class ReplicationClientFactory(ReconnectingClientFactory):
    maxDelay = 5

    def __init__(self, hs, client_name, handler):
        self.client_name = client_name
        self.handler = handler
        self.server_name = hs.config.server_name
        self._clock = hs.get_clock()  # As self.clock is defined in super class

        reactor.addSystemEventTrigger("before", "shutdown", self.stopTrying)

    def startedConnecting(self, connector):
        logger.info("Connecting to replication: %r", connector.getDestination())

    def buildProtocol(self, addr):
        logger.info("Connected to replication: %r", addr)
        self.resetDelay()
        return ClientReplicationStreamProtocol(
            self.client_name, self.server_name, self._clock, self.handler
        )

    def clientConnectionLost(self, connector, reason):
        logger.error("Lost replication conn: %r", reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        logger.error("Failed to connect to replication: %r", reason)
        ReconnectingClientFactory.clientConnectionFailed(
            self, connector, reason
        )


class ReplicationHandler(object):
    def __init__(self, hs, client_name):
        self.replication_host = hs.config.worker_replication_host
        self.replication_port = hs.config.worker_replication_port
        self.factory = ReplicationClientFactory(hs, client_name, self)

        self.store = hs.get_datastore()

        self.connection = None

    def start_replication(self):
        reactor.connectTCP(self.replication_host, self.replication_port, self.factory)

    def on_rdata(self, stream_name, token, row):
        logger.info("Received rdata %s -> %s", stream_name, token)
        self.store.process_replication_row(stream_name, token, row)

    def on_position(self, stream_name, token):
        self.store.process_replication_row(stream_name, token, None)

    def get_streams_to_replicate(self):
        args = self.store.stream_positions()
        user_account_data = args.pop("user_account_data", None)
        room_account_data = args.pop("room_account_data", None)
        if user_account_data:
            args["account_data"] = user_account_data
        elif room_account_data:
            args["account_data"] = room_account_data
        return args

    def send_federation_ack(self, token):
        if self.connection:
            self.connection.send_command(FederationAckCommand(token))
        else:
            logger.warn("Dropping federation ack as we are disconnected from master")

    def update_connection(self, connection):
        self.connection = connection
