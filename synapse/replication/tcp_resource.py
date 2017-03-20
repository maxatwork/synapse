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


SERVER = "SERVER"
RDATA = "RDATA"
POSITION = "POSITION"
ERROR = "ERROR"
PING = "PING"

REPLICATE = "REPLICATE"
NAME = "NAME"
USER_SYNC = "USER_SYNC"

VALID_SERVER_COMMANDS = (SERVER, RDATA, POSITION, ERROR, PING,)
VALID_CLIENT_COMMANDS = (NAME, REPLICATE, PING, USER_SYNC,)


MAX_EVENTS_BEHIND = 10000


class ReplicationStreamProtocolFactory(Factory):
    def __init__(self, hs):
        self.streamer = ReplicationStreamer(hs)
        self.clock = hs.get_clock()
        self.server_name = hs.config.server_name

    def buildProtocol(self, addr):
        return ReplicationStreamProtocol(
            self.server_name,
            self.clock,
            self.streamer,
            addr
        )


class ReplicationStreamProtocol(LineOnlyReceiver):
    delimiter = b'\n'

    def __init__(self, server_name, clock, streamer, addr):
        self.server_name = server_name
        self.clock = clock
        self.streamer = streamer
        self.addr = addr

        self.name = None

        self.replication_streams = set()
        self.connecting_streams = set()
        self.pending_rdata = {}

        self.last_received_command = self.clock.time_msec()
        self.last_sent_command = 0

    def connectionMade(self):
        self.streamer.connections.append(self)
        self.send_command(SERVER, self.server_name)

    def lineReceived(self, line):
        if line.strip() == "":
            # Ignore blank lines
            return

        cmd, rest_of_line = line.split(" ", 1)

        if cmd not in VALID_CLIENT_COMMANDS:
            self.send_error("unkown command: %s", cmd)
            return

        self.last_received_command = self.clock.time_msec()

        getattr(self, "on_%s" % (cmd,))(rest_of_line)

    def send_error(self, error_string, *args):
        self.send_command("ERROR", error_string % args)
        self.transport.loseConnection()

    def send_command(self, cmd, *values):
        if cmd not in VALID_SERVER_COMMANDS:
            raise Exception("Invalid command %r", cmd)

        string = "%s %s" % (cmd, " ".join(str(value) for value in values),)
        self.sendLine(string)

        self.last_sent_command = self.clock.time_msec()

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
                token, row = update[0], update[1]
                self.send_command(RDATA, stream_name, token, row)

            pending_rdata = self.pending_rdata.pop(stream_name, [])
            for token, update in pending_rdata:
                self.send_command(RDATA, stream_name, token, update)

            self.send_command(POSITION, stream_name, current_token)

            self.replication_streams.add(stream_name)
        except Exception as e:
            logger.exception("Failed to handle REPLICATE command")
            self.send_error("failed to handle replicate: %r", e)
        finally:
            self.connecting_streams.discard(stream_name)

    def on_PING(self, line):
        pass

    def on_USER_SYNC(self, line):
        state, user_id = line.split(" ", 1)

        if state not in ("start", "end"):
            self.send_error("invalide USER_SYNC state")
            return

        self.streamer.on_user_sync(user_id, state)

    def stream_update(self, stream, token, data):
        if stream in self.replication_streams:
            self.send_command(RDATA, stream, token, data)
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

        self.streams = [
            EventsStream(hs),
            BackfillStream(hs),
            PresenceStream(hs),
            TypingStream(hs),
            ReceiptsStream(hs),
            PushRulesStream(hs),
            PushersStream(hs),
            CachesStream(hs),
            PublicRoomsStream(hs),
            DeviceListsStream(hs),
            ToDeviceStream(hs),
            TagAccountDataStream(hs),
            AccountDataStream(hs),
        ]

        if not hs.config.send_federation:
            self.streams.append(FederationStream(hs))

        self.streams_by_name = {stream.NAME: stream for stream in self.streams}

        self.notifier_listener()

        self.is_looping = False
        self.pending_updates = False

        self.clock = hs.get_clock()
        self.clock.looping_call(self.send_ping, 5000)

    def send_ping(self):
        now = self.clock.time_msec()
        for connection in self.connections:
            if now - connection.last_sent_command > 5000:
                connection.send_command(PING, now)

    @defer.inlineCallbacks
    def notifier_listener(self):
        while True:
            yield self.notifier.wait_once_for_replication()
            logger.debug("Woken up by notifier")
            self.on_notifier_poke()

    @defer.inlineCallbacks
    def on_notifier_poke(self):
        if not self.connections:
            return

        if self.is_looping:
            logger.debug("Noitifier poke loop already running")
            self.pending_updates = True
            return

        self.pending_updates = False
        self.is_looping = True

        try:
            while True:
                for stream in self.streams:
                    stream.advance_current_token()

                self.pending_updates = False

                for stream in self.streams:
                    logger.debug("Getting stream: %s", stream.NAME)
                    updates, current_token = yield stream.get_updates()

                    logger.debug(
                        "Sending %d updates to %d connections",
                        len(updates), len(self.connections),
                    )

                    for update in updates:
                        logger.debug("Streaming: %r", update)
                        token, row = update[0], update[1]
                        for conn in self.connections:
                            try:
                                conn.stream_update(stream.NAME, token, row)
                            except Exception:
                                logger.exception("Failed to replicate")

                if not self.pending_updates:
                    logger.debug("No more pending updates, breaking poke loop")
                    break
        finally:
            self.pending_updates = False
            self.is_looping = False

    def get_stream_updates(self, stream_name, token):
        stream = self.streams_by_name.get(stream_name, None)
        if not stream:
            raise Exception("unknown stream %s", stream_name)

        return stream.get_updates_since(token)


class Stream(object):
    NAME = None
    _LIMITED = True

    def __init__(self, hs):
        self.last_token = self.current_token()
        self.upto_token = self.current_token()

    def advance_current_token(self):
        self.upto_token = self.current_token()

    @defer.inlineCallbacks
    def get_updates(self):
        updates, current_token = yield self.get_updates_since(self.last_token)
        self.last_token = current_token

        defer.returnValue((updates, current_token))

    @defer.inlineCallbacks
    def get_updates_since(self, from_token):
        if from_token in ("NOW", "now"):
            defer.returnValue(([], self.upto_token))

        current_token = self.upto_token

        from_token = long(from_token)

        if from_token == current_token:
            defer.returnValue(([], current_token))

        if self._LIMITED:
            rows = yield self.update_function(
                from_token, current_token,
                limit=MAX_EVENTS_BEHIND + 1,
            )

            if len(rows) >= MAX_EVENTS_BEHIND:
                raise Exception("stream %s has fallen behined" % (self.NAME))
        else:
            rows = yield self.update_function(
                from_token, current_token,
            )

        updates = [(row[0], json.dumps(row[1:])) for row in rows]

        defer.returnValue((updates, current_token))

    def current_token():
        raise NotImplementedError()

    def update_function():
        raise NotImplementedError()


class EventsStream(Stream):
    NAME = "events"

    def __init__(self, hs):
        store = hs.get_datastore()
        self.current_token = store.get_current_events_token
        self.update_function = store.get_all_new_forward_event_rows

        super(EventsStream, self).__init__(hs)


class BackfillStream(Stream):
    NAME = "backfill"

    def __init__(self, hs):
        store = hs.get_datastore()
        self.current_token = store.get_current_backfill_token
        self.update_function = store.get_all_new_backfill_event_rows

        super(BackfillStream, self).__init__(hs)


class PresenceStream(Stream):
    NAME = "presence"
    _LIMITED = False

    def __init__(self, hs):
        store = hs.get_datastore()
        presence_handler = hs.get_presence_handler()

        self.current_token = store.get_current_presence_token
        self.update_function = presence_handler.get_all_presence_updates

        super(PresenceStream, self).__init__(hs)


class TypingStream(Stream):
    NAME = "typing"
    _LIMITED = False

    def __init__(self, hs):
        typing_handler = hs.get_typing_handler()

        self.current_token = typing_handler.get_current_token
        self.update_function = typing_handler.get_all_typing_updates

        super(TypingStream, self).__init__(hs)


class ReceiptsStream(Stream):
    NAME = "receipts"

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_max_receipt_stream_id
        self.update_function = store.get_all_updated_receipts

        super(ReceiptsStream, self).__init__(hs)


class PushRulesStream(Stream):
    NAME = "push_rules"

    def __init__(self, hs):
        self.store = hs.get_datastore()

        self.update_function = self.store.get_all_push_rule_updates

        super(PushRulesStream, self).__init__(hs)

    def current_token(self):
        push_rules_token, _ = self.store.get_push_rules_stream_token()
        return push_rules_token


class PushersStream(Stream):
    NAME = "pushers"

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_pushers_stream_token
        self.update_function = store.get_all_updated_pushers

        super(PushersStream, self).__init__(hs)


class CachesStream(Stream):
    NAME = "caches"

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_cache_stream_token
        self.update_function = store.get_all_updated_caches

        super(CachesStream, self).__init__(hs)


class PublicRoomsStream(Stream):
    NAME = "public_rooms"

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_current_public_room_stream_id
        self.update_function = store.get_all_new_public_rooms

        super(PublicRoomsStream, self).__init__(hs)


class DeviceListsStream(Stream):
    NAME = "device_lists"
    _LIMITED = False

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_device_stream_token
        self.update_function = store.get_all_device_list_changes_for_remotes

        super(DeviceListsStream, self).__init__(hs)


class ToDeviceStream(Stream):
    NAME = "to_device"

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_device_stream_token
        self.update_function = store.get_all_new_device_messages

        super(ToDeviceStream, self).__init__(hs)


class FederationStream(Stream):
    NAME = "federation"

    def __init__(self, hs):
        federation_sender = hs.get_federation_sender()

        self.current_token = federation_sender.get_current_token
        self.update_function = federation_sender.get_replication_rows

        super(FederationStream, self).__init__(hs)


class TagAccountDataStream(Stream):
    NAME = "tag_account_data"

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_max_account_data_stream_id
        self.update_function = store.get_all_updated_tags

        super(TagAccountDataStream, self).__init__(hs)


class AccountDataStream(Stream):
    NAME = "account_data"

    def __init__(self, hs):
        self.store = hs.get_datastore()

        self.current_token = self.store.get_max_account_data_stream_id

        super(AccountDataStream, self).__init__(hs)

    @defer.inlineCallbacks
    def update_function(self, from_token, to_token, limit):
        global_results, room_results = yield self.store.get_all_updated_account_data(
            from_token, from_token, to_token, limit
        )

        results = list(room_results)
        results.extend(
            (stream_id, user_id, None, account_data_type, content,)
            for stream_id, user_id, account_data_type, content in global_results
        )

        defer.returnValue(results)
