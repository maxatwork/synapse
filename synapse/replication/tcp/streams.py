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
from collections import namedtuple

import logging


logger = logging.getLogger(__name__)


MAX_EVENTS_BEHIND = 10000


EventStreamRow = namedtuple("EventStreamRow",
                            ("event_id", "room_id", "type", "state_key", "redacts"))
BackfillStreamRow = namedtuple("BackfillStreamRow",
                               ("event_id", "room_id", "type", "state_key", "redacts"))
PresenceStreamRow = namedtuple("PresenceStreamRow",
                               ("user_id", "state", "last_active_ts",
                                "last_federation_update_ts", "last_user_sync_ts",
                                "status_msg", "currently_active"))
TypingStreamRow = namedtuple("TypingStreamRow",
                             ("room_id", "user_ids"))
ReceiptsStreamRow = namedtuple("ReceiptsStreamRow",
                               ("room_id", "receipt_type", "user_id", "event_id",
                                "data"))
PushRulesStreamRow = namedtuple("PushRulesStreamRow", ("user_id",))
PushersStreamRow = namedtuple("PushersStreamRow",
                              ("user_id", "app_id", "pushkey", "deleted",))
CachesStreamRow = namedtuple("CachesStreamRow",
                             ("cache_func", "keys", "invalidation_ts",))
PublicRoomsStreamRow = namedtuple("PublicRoomsStreamRow",
                                  ("room_id", "visibility", "appservice_id",
                                   "network_id",))
DeviceListsStreamRow = namedtuple("DeviceListsStreamRow", ("user_id", "destination",))
ToDeviceStreamRow = namedtuple("ToDeviceStreamRow", ("entity",))
FederationStreamRow = namedtuple("FederationStreamRow", ("type", "data",))
TagAccountDataStreamRow = namedtuple("TagAccountDataStreamRow",
                                     ("user_id", "room_id", "data"))
AccountDataStreamRow = namedtuple("AccountDataStream",
                                  ("user_id", "room_id", "data_type", "data"))


class Stream(object):
    NAME = None
    ROW_TYPE = None
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

        from_token = int(from_token)

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

        updates = [(row[0], self.ROW_TYPE(*row[1:])) for row in rows]

        defer.returnValue((updates, current_token))

    def current_token():
        raise NotImplementedError()

    def update_function():
        raise NotImplementedError()


class EventsStream(Stream):
    NAME = "events"
    ROW_TYPE = EventStreamRow

    def __init__(self, hs):
        store = hs.get_datastore()
        self.current_token = store.get_current_events_token
        self.update_function = store.get_all_new_forward_event_rows

        super(EventsStream, self).__init__(hs)


class BackfillStream(Stream):
    NAME = "backfill"
    ROW_TYPE = BackfillStreamRow

    def __init__(self, hs):
        store = hs.get_datastore()
        self.current_token = store.get_current_backfill_token
        self.update_function = store.get_all_new_backfill_event_rows

        super(BackfillStream, self).__init__(hs)


class PresenceStream(Stream):
    NAME = "presence"
    _LIMITED = False
    ROW_TYPE = PresenceStreamRow

    def __init__(self, hs):
        store = hs.get_datastore()
        presence_handler = hs.get_presence_handler()

        self.current_token = store.get_current_presence_token
        self.update_function = presence_handler.get_all_presence_updates

        super(PresenceStream, self).__init__(hs)


class TypingStream(Stream):
    NAME = "typing"
    _LIMITED = False
    ROW_TYPE = TypingStreamRow

    def __init__(self, hs):
        typing_handler = hs.get_typing_handler()

        self.current_token = typing_handler.get_current_token
        self.update_function = typing_handler.get_all_typing_updates

        super(TypingStream, self).__init__(hs)


class ReceiptsStream(Stream):
    NAME = "receipts"
    ROW_TYPE = ReceiptsStreamRow

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_max_receipt_stream_id
        self.update_function = store.get_all_updated_receipts

        super(ReceiptsStream, self).__init__(hs)


class PushRulesStream(Stream):
    NAME = "push_rules"
    ROW_TYPE = PushRulesStreamRow

    def __init__(self, hs):
        self.store = hs.get_datastore()

        self.update_function = self.store.get_all_push_rule_updates

        super(PushRulesStream, self).__init__(hs)

    def current_token(self):
        push_rules_token, _ = self.store.get_push_rules_stream_token()
        return push_rules_token


class PushersStream(Stream):
    NAME = "pushers"
    ROW_TYPE = PushersStreamRow

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_pushers_stream_token
        self.update_function = store.get_all_updated_pushers_rows

        super(PushersStream, self).__init__(hs)


class CachesStream(Stream):
    NAME = "caches"
    ROW_TYPE = CachesStreamRow

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_cache_stream_token
        self.update_function = store.get_all_updated_caches

        super(CachesStream, self).__init__(hs)


class PublicRoomsStream(Stream):
    NAME = "public_rooms"
    ROW_TYPE = PublicRoomsStreamRow

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_current_public_room_stream_id
        self.update_function = store.get_all_new_public_rooms

        super(PublicRoomsStream, self).__init__(hs)


class DeviceListsStream(Stream):
    NAME = "device_lists"
    _LIMITED = False
    ROW_TYPE = DeviceListsStreamRow

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_device_stream_token
        self.update_function = store.get_all_device_list_changes_for_remotes

        super(DeviceListsStream, self).__init__(hs)


class ToDeviceStream(Stream):
    NAME = "to_device"
    ROW_TYPE = ToDeviceStreamRow

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_device_stream_token
        self.update_function = store.get_all_new_device_messages

        super(ToDeviceStream, self).__init__(hs)


class FederationStream(Stream):
    NAME = "federation"
    ROW_TYPE = FederationStreamRow

    def __init__(self, hs):
        federation_sender = hs.get_federation_sender()

        self.current_token = federation_sender.get_current_token
        self.update_function = federation_sender.get_replication_rows

        super(FederationStream, self).__init__(hs)


class TagAccountDataStream(Stream):
    NAME = "tag_account_data"
    ROW_TYPE = TagAccountDataStreamRow

    def __init__(self, hs):
        store = hs.get_datastore()

        self.current_token = store.get_max_account_data_stream_id
        self.update_function = store.get_all_updated_tags

        super(TagAccountDataStream, self).__init__(hs)


class AccountDataStream(Stream):
    NAME = "account_data"
    ROW_TYPE = AccountDataStreamRow

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


STREAMS_MAP = {
    stream.NAME: stream
    for stream in (
        EventsStream,
        BackfillStream,
        PresenceStream,
        TypingStream,
        ReceiptsStream,
        PushRulesStream,
        PushersStream,
        CachesStream,
        PublicRoomsStream,
        DeviceListsStream,
        ToDeviceStream,
        FederationStream,
        TagAccountDataStream,
        AccountDataStream,
    )
}
