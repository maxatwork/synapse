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
)

import logging


logger = logging.getLogger(__name__)


class BaseReplicationStreamProtocol(LineOnlyReceiver):
    delimiter = b'\n'

    def __init__(self, server_name, clock):
        self.server_name = server_name
        self.clock = clock

        self.last_received_command = self.clock.time_msec()
        self.last_sent_command = 0

        self.clock.looping_call(self.send_ping, 5000)

    def send_ping(self):
        now = self.clock.time_msec()
        if now - self.last_sent_command > 5000:
            self.send_command(PingCommand(now))

    def lineReceived(self, line):
        if line.strip() == "":
            # Ignore blank lines
            return

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

        getattr(self, "on_%s" % (cmd_name,))(cmd)

    def send_error(self, error_string, *args):
        self.send_command(ErrorCommand(error_string % args))
        self.transport.loseConnection()

    def send_command(self, cmd):
        string = "%s %s" % (cmd.NAME, cmd.to_line(),)
        self.sendLine(string)

        self.last_sent_command = self.clock.time_msec()

    def on_PING(self, line):
        pass


class ReplicationStreamProtocol(BaseReplicationStreamProtocol):
    VALID_INBOUND_COMMANDS = VALID_CLIENT_COMMANDS
    VALID_OUTBOUND_COMMANDS = VALID_SERVER_COMMANDS

    def __init__(self, server_name, clock, streamer, addr):
        BaseReplicationStreamProtocol.__init__(self, server_name, clock)

        self.streamer = streamer
        self.addr = addr

        self.name = None

        self.replication_streams = set()
        self.connecting_streams = set()
        self.pending_rdata = {}

    def connectionMade(self):
        self.streamer.connections.append(self)
        self.send_command(ServerCommand(self.server_name))

    def on_NAME(self, cmd):
        self.name = cmd.data

    def on_USER_SYNC(self, cmd):
        self.streamer.on_user_sync(cmd.user_id, cmd.state)

    def on_REPLICATE(self, cmd):
        stream_name = cmd.stream_name
        token = cmd.token

        if stream_name == "ALL":
            for stream in STREAMS_MAP.iterkeys():
                self.subscripe_to_stream(stream, token)
        else:
            self.subscripe_to_stream(stream_name, token)

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
            self.pending_rdata.setdefault(stream_name, []).append((token, data))

    def connectionLost(self, reason):
        try:
            self.streamer.connections.remove(self)
        except:
            pass

        logger.info("Replication connection lost: %r", self)

    def __str__(self):
        return "ReplicationConnection<name=%s,addr=%s>" % (self.name, self.addr)
