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

from twisted.internet import defer
from twisted.protocols.basic import LineOnlyReceiver

from commands import (
    COMMAND_MAP, VALID_CLIENT_COMMANDS, VALID_SERVER_COMMANDS,
    ErrorCommand, ServerCommand, RdataCommand, PositionCommand, PingCommand,
    NameCommand, ReplicateCommand, UserSyncCommand
)
from streams import STREAMS_MAP

import logging


logger = logging.getLogger(__name__)


PING_TIME = 5000


next_conn_id = 1


class BaseReplicationStreamProtocol(LineOnlyReceiver):
    delimiter = b'\n'

    def __init__(self, clock):
        self.clock = clock

        self.last_received_command = self.clock.time_msec()
        self.last_sent_command = 0

        self.received_ping = False

        self.connection_established = False

        self.pending_commands = []

    def connectionMade(self):
        self.connection_established = True

        for cmd in self.pending_commands:
            self.send_command(cmd)

        self.pending_commands = []

        self.clock.looping_call(self.send_ping, 5000)
        self.send_ping()

    def send_ping(self):
        now = self.clock.time_msec()
        if now - self.last_sent_command >= PING_TIME:
            self.send_command(PingCommand(now))

        if self.received_ping and now - self.last_received_command > PING_TIME * 3:
            logger.info(
                "Connection hasn't received command in %r ms. Closing.",
                now - self.last_received_command
            )
            self.send_error("ping timeout")

    def lineReceived(self, line):
        if line.strip() == "":
            # Ignore blank lines
            return

        line = line.decode("utf-8")
        cmd_name, rest_of_line = line.split(" ", 1)

        if cmd_name not in self.VALID_INBOUND_COMMANDS:
            self.send_error("invalid command: %s", cmd_name)
            return

        self.last_received_command = self.clock.time_msec()

        cmd_cls = COMMAND_MAP[cmd_name]
        try:
            cmd = cmd_cls.from_line(rest_of_line)
        except Exception as e:
            logger.exception("failed to parse line %r: %r", cmd_name, rest_of_line)
            self.send_error(
                "failed to parse line for  %r: %r (%r):" % (cmd_name, e, rest_of_line)
            )
            return

        try:
            getattr(self, "on_%s" % (cmd_name,))(cmd)
        except Exception:
            logger.exception("Failed to handle line: %r", line)

    def send_error(self, error_string, *args):
        self.send_command(ErrorCommand(error_string % args))
        self.transport.loseConnection()

    def send_command(self, cmd):
        if not self.connection_established:
            logger.info("Queing as conn not ready %r", cmd)
            self.pending_commands.append(cmd)
            return

        string = "%s %s" % (cmd.NAME, cmd.to_line(),)
        if "\n" in string:
            raise Exception("Unexpected newline in command: %r", string)

        self.sendLine(string.encode("utf-8"))

        self.last_sent_command = self.clock.time_msec()

    def on_PING(self, line):
        self.received_ping = True


class ServerReplicationStreamProtocol(BaseReplicationStreamProtocol):
    VALID_INBOUND_COMMANDS = VALID_CLIENT_COMMANDS
    VALID_OUTBOUND_COMMANDS = VALID_SERVER_COMMANDS

    def __init__(self, server_name, clock, streamer, addr):
        BaseReplicationStreamProtocol.__init__(self, clock)

        self.server_name = server_name
        self.streamer = streamer
        self.addr = addr

        self.name = None

        self.replication_streams = set()
        self.connecting_streams = set()
        self.pending_rdata = {}

        global next_conn_id
        self.conn_id = next_conn_id
        next_conn_id += 1

    def connectionMade(self):
        self.send_command(ServerCommand(self.server_name))
        BaseReplicationStreamProtocol.connectionMade(self)
        self.streamer.new_connection(self)

    def on_NAME(self, cmd):
        self.name = cmd.data

    def on_USER_SYNC(self, cmd):
        self.streamer.on_user_sync(self.conn_id, cmd.user_id, cmd.is_syncing)

    def on_REPLICATE(self, cmd):
        stream_name = cmd.stream_name
        token = cmd.token

        if stream_name == "ALL":
            # Subscribe to all streams we're publishing to.
            for stream in self.streamer.streams_by_name.iterkeys():
                self.subscripe_to_stream(stream, token)
        else:
            self.subscripe_to_stream(stream_name, token)

    def on_FEDERATION_ACK(self, cmd):
        self.streamer.federation_ack(cmd.token)

    @defer.inlineCallbacks
    def subscripe_to_stream(self, stream_name, token):
        self.replication_streams.discard(stream_name)
        self.connecting_streams.add(stream_name)

        try:
            updates, current_token = yield self.streamer.get_stream_updates(
                stream_name, token,
            )

            for update in updates:
                token, row = update[0], update[1]
                self.send_command(RdataCommand(stream_name, token, row))

            pending_rdata = self.pending_rdata.pop(stream_name, [])
            for token, update in pending_rdata:
                self.send_command(RdataCommand(stream_name, token, update))

            self.send_command(PositionCommand(stream_name, current_token))

            self.replication_streams.add(stream_name)
        except Exception as e:
            logger.exception("Failed to handle REPLICATE command")
            self.send_error("failed to handle replicate: %r", e)
        finally:
            self.connecting_streams.discard(stream_name)

    def stream_update(self, stream_name, token, data):
        if stream_name in self.replication_streams:
            self.send_command(RdataCommand(stream_name, token, data))
        elif stream_name in self.connecting_streams:
            logger.info("Queuing RDATA %r %r", stream_name, token)
            self.pending_rdata.setdefault(stream_name, []).append((token, data))
        else:
            logger.debug("Dropping RDATA %r %r", stream_name, token)

    def connectionLost(self, reason):
        logger.info("Replication connection lost: %r: %r", self, reason)
        self.streamer.lost_connection(self)

    def __str__(self):
        return "ReplicationConnection<name=%s,addr=%s>" % (self.name, self.addr)


class ClientReplicationStreamProtocol(BaseReplicationStreamProtocol):
    VALID_INBOUND_COMMANDS = VALID_SERVER_COMMANDS
    VALID_OUTBOUND_COMMANDS = VALID_CLIENT_COMMANDS

    def __init__(self, client_name, server_name, clock, handler):
        BaseReplicationStreamProtocol.__init__(self, clock)

        self.client_name = client_name
        self.server_name = server_name
        self.handler = handler

        self.pending_batches = {}

    def connectionMade(self):
        self.send_command(NameCommand(self.client_name))
        BaseReplicationStreamProtocol.connectionMade(self)

        for stream_name, token in self.handler.get_streams_to_replicate().iteritems():
            self.replicate(stream_name, token)

        currently_syncing = self.handler.get_currently_syncing_users()
        for user_id in currently_syncing:
            self.send_command(UserSyncCommand(user_id, True))

        self.handler.update_connection(self)

    def on_SERVER(self, cmd):
        if cmd.data != self.server_name:
            logger.error("Connected to wrong remote: %r", cmd.data)
            self.transport.abortConnection()

    def on_ERROR(self, cmd):
        logger.error("Remote reported error: %r", cmd.data)

    def on_RDATA(self, cmd):
        try:
            row = STREAMS_MAP[cmd.stream_name].ROW_TYPE(*cmd.row)
        except Exception:
            logger.exception("Failed to parse RDATA: %r %r", cmd.stream_name, cmd.row)
            raise

        if cmd.token is None:
            self.pending_batches.setdefault(cmd.stream_name, []).append(row)
        else:
            rows = self.pending_batches.pop(cmd.stream_name, [])
            rows.append(row)
            self.handler.on_rdata(cmd.stream_name, cmd.token, rows)

    def on_POSITION(self, cmd):
        self.handler.on_position(cmd.stream_name, cmd.token)

    def replicate(self, stream_name, token):
        if stream_name not in STREAMS_MAP:
            raise Exception("Invalid stream name %r" % (stream_name,))

        logger.info("Subscribing to replication stream: %r from %r", stream_name, token)

        self.send_command(ReplicateCommand(stream_name, token))

    def connectionLost(self, reason):
        logger.info("Replication connection lost: %r: %r", self, reason)
        self.handler.update_connection(None)
