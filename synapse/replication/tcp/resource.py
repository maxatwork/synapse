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

from twisted.internet import defer, reactor
from twisted.internet.protocol import Factory

from streams import STREAMS_MAP, FederationStream
from protocol import ServerReplicationStreamProtocol

import logging


logger = logging.getLogger(__name__)


MAX_EVENTS_BEHIND = 10000


class ReplicationStreamProtocolFactory(Factory):
    def __init__(self, hs):
        self.streamer = ReplicationStreamer(hs)
        self.clock = hs.get_clock()
        self.server_name = hs.config.server_name

    def buildProtocol(self, addr):
        return ServerReplicationStreamProtocol(
            self.server_name,
            self.clock,
            self.streamer,
            addr
        )


class ReplicationStreamer(object):
    def __init__(self, hs):
        self.store = hs.get_datastore()
        self.notifier = hs.get_notifier()

        self.connections = []

        self.streams = [
            stream(hs) for stream in STREAMS_MAP.itervalues()
            if stream != FederationStream or not hs.config.send_federation
        ]

        self.streams_by_name = {stream.NAME: stream for stream in self.streams}

        self.notifier_listener()

        self.is_looping = False
        self.pending_updates = False

        reactor.addSystemEventTrigger("before", "shutdown", self.on_shutdown)

    def on_shutdown(self):
        for conn in self.connections:
            conn.send_error("server shutting down")

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
                self.pending_updates = False

                for stream in self.streams:
                    stream.advance_current_token()

                for stream in self.streams:
                    logger.debug(
                        "Getting stream: %s: %s -> %s",
                        stream.NAME, stream.last_token, stream.upto_token
                    )
                    updates, current_token = yield stream.get_updates()

                    logger.debug(
                        "Sending %d updates to %d connections",
                        len(updates), len(self.connections),
                    )

                    if updates:
                        logger.info("Streaming: %s -> %s", stream.NAME, updates[-1][0])

                    for update in updates:
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
