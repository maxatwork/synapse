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
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineOnlyReceiver

import logging
import ujson as json


logger = logging.getLogger(__name__)


RDATA = "RDATA"
POSITION = "POSITION"
ERROR = "ERROR"
PING = "PING"

REPLICATE = "REPLICATE"
NAME = "NAME"
USER_SYNC = "USER_SYNC"

VALID_SERVER_COMMANDS = (RDATA, POSITION, ERROR, PING,)
VALID_CLIENT_COMMANDS = (NAME, REPLICATE, PING, USER_SYNC,)


MAX_EVENTS_BEHIND = 10000


class ReplicationStreamProtocolFactory(Factory):
    def __init__(self, hs):
        self.streamer = ReplicationStreamer(hs)

    def buildProtocol(self, addr):
        return ReplicationStreamProtocol(self.streamer, addr)


class ReplicationStreamProtocol(LineOnlyReceiver):
    delimiter = b'\n'

    def __init__(self, streamer, addr):
        self.addr = addr
        self.name = None

        self.replication_streams = set()
        self.connecting_streams = set()
        self.pending_rdata = {}

        self.streamer = streamer
        self.streamer.connections.append(self)

    def lineReceived(self, line):
        cmd, rest_of_line = line.split(" ", 1)

        if cmd not in VALID_CLIENT_COMMANDS:
            self.send_error("unkown command: %s", cmd)
            return

        getattr(self, "on_%s" % (cmd,))(rest_of_line)

    def send_error(self, error_string, *args):
        self.send_command("ERROR", error_string % args)
        self.transport.loseConnection()

    def send_command(self, cmd, *values):
        if cmd not in VALID_SERVER_COMMANDS:
            raise Exception("Invalid command %r", cmd)

        string = "%s %s" % (cmd, " ".join(values),)
        self.sendLine(string)

    def on_NAME(self, line):
        self.name = line

    @defer.inlineCallbacks
    def on_REPLICATE(self, line):
        stream_name, token = line.split(" ", 1)

        self.replication_streams.discard(stream_name)
        self.connecting_streams.add(stream_name)

        try:
            updates, current_token = yield self.streamer.get_stream_updates(
                stream_name, token,
            )

            for update in updates:
                self.send_command(RDATA, stream_name, *update)

            pending_rdata = self.pending_rdata.pop(stream_name, [])
            for token, update in pending_rdata:
                self.send_command(RDATA, stream_name, token, *update)

            self.send_command(POSITION, stream_name, current_token)

            self.replication_streams.add(stream_name)
        except Exception as e:
            self.send_error("failed to handle replicate: %r", e)
        finally:
            self.connecting_streams.discard(stream_name)

    def on_PING(self, line):
        self.last_received_ping = self.clock.time_msec()

    def on_USER_SYNC(self, line):
        state, user_id = line.split(" ", 1)

        if state not in ("start", "end"):
            self.send_error("invalide USER_SYNC state")
            return

        self.streamer.on_user_sync(user_id, state)

    def stream_update(self, stream, token, *data):
        if stream in self.replication_streams:
            self.send_command(RDATA, stream, token, *data)
        elif stream in self.connecting_streams:
            self.pending_rdata.setdefault(stream, []).append((token, data))

    def connectionLost(self, reason):
        try:
            self.streamer.connections.remove(self)
        except:
            pass

        logger.info("Replication connection lost: %r", self)

    def __str__(self):
        return "ReplicationConnection<name=%s,addr=%s>" % (self.name, self.addr)


class ReplicationStreamer(object):
    def __init__(self, hs):
        self.store = hs.get_datastore()
        self.notifier = hs.get_notifier()

        self.connections = []

        self.last_event_stream_token = self.get_events_current_token()

        self.notifier_listener()

        self.is_looping = False
        self.pending_updates = False

    @defer.inlineCallbacks
    def notifier_listener(self):
        while True:
            yield self.notifier.wait_once_for_replication()
            logger.debug("Woken up by notifier")
            self.on_notifier_poke()

    @defer.inlineCallbacks
    def on_notifier_poke(self):
        if self.is_looping:
            logger.debug("Noitifier poke loop already running")
            self.pending_updates = True
            return

        self.pending_updates = False
        self.is_looping = True

        try:
            while True:
                self.pending_updates = False
                logger.debug("Getting event stream")
                updates, current_token = yield self.get_events_stream(
                    self.last_event_stream_token
                )
                self.last_event_stream_token = current_token

                logger.debug(
                    "Sending %d updates to %d connections",
                    len(updates), len(self.connections),
                )

                for update in updates:
                    logger.debug("Streaming: %r", update)
                    for conn in self.connections:
                        try:
                            conn.stream_update("events", *update)
                        except Exception:
                            logger.exception("Failed to replicate")

                if not self.pending_updates:
                    logger.debug("No more pending updates, breaking poke loop")
                    break
        finally:
            self.pending_updates = False
            self.is_looping = False

    def get_stream_updates(self, stream_name, token):
        if stream_name == "events":
            return self.get_events_stream(token)

    @defer.inlineCallbacks
    def get_events_stream(self, token):
        curr_backfill = self.store.get_current_backfill_token()
        _, curr_events = self.store.get_push_rules_stream_token()

        if token != "NOW":
            request_events, request_backfill = token.split("_")
        else:
            request_events = curr_events
            request_backfill = curr_backfill
        res = yield self.store.get_all_new_event_rows(
            long(request_backfill), long(request_events),
            curr_backfill, curr_events,
            limit=MAX_EVENTS_BEHIND + 1,
        )

        updates = []

        for row in res.new_forward_events:
            update = [
                "%s_%s" % (row[0], request_backfill),
                json.dumps(row[1:] + [False]),
            ]
            updates.append(update)

        for row in res.new_backfill_events:
            update = [
                "%s_%s" % (curr_events, row[0]),
                json.dumps(row[1:] + [True]),
            ]
            updates.append(update)

        current_token = "%s_%s" % (curr_events, curr_backfill,)
        defer.returnValue((updates, current_token))

    def get_events_current_token(self):
        curr_backfill = self.store.get_current_backfill_token()
        _, curr_events = self.store.get_push_rules_stream_token()
        return "%s_%s" % (curr_events, curr_backfill,)


def _position_from_rows(rows, current_position):
    if rows:
        return rows[-1][0]
    return current_position
