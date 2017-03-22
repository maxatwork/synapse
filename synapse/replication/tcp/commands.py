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

import logging
import ujson as json


logger = logging.getLogger(__name__)


class Command(object):
    NAME = ""

    def __init__(self, data):
        self.data = data

    @classmethod
    def from_line(cls, line):
        return cls(line)

    def to_line(self):
        return self.data


class ServerCommand(Command):
    NAME = "SERVER"


class RdataCommand(Command):
    NAME = "RDATA"

    def __init__(self, stream_name, token, row):
        self.stream_name = stream_name
        self.token = token
        self.row = row

    @classmethod
    def from_line(cls, line):
        stream_name, token, row_json = line.split(" ", 2)
        return cls(
            stream_name,
            None if token == "batch" else int(token),
            json.loads(row_json)
        )

    def to_line(self):
        return " ".join((
            self.stream_name,
            str(self.token) if self.token is not None else "batch",
            json.dumps(self.row),
        ))


class PositionCommand(Command):
    NAME = "POSITION"

    def __init__(self, stream_name, token):
        self.stream_name = stream_name
        self.token = token

    @classmethod
    def from_line(cls, line):
        stream_name, token = line.split(" ", 1)
        return cls(stream_name, int(token))

    def to_line(self):
        return " ".join((self.stream_name, str(self.token),))


class ErrorCommand(Command):
    NAME = "ERROR"


class PingCommand(Command):
    NAME = "PING"


class NameCommand(Command):
    NAME = "NAME"


class ReplicateCommand(Command):
    NAME = "REPLICATE"

    def __init__(self, stream_name, token):
        self.stream_name = stream_name
        self.token = token

    @classmethod
    def from_line(cls, line):
        stream_name, token = line.split(" ", 1)
        if token in ("NOW", "now"):
            token = "NOW"
        else:
            token = int(token)
        return cls(stream_name, token)

    def to_line(self):
        return " ".join((self.stream_name, str(self.token),))


class UserSyncCommand(Command):
    NAME = "USER_SYNC"

    def __init__(self, user_id, is_syncing):
        self.user_id = user_id
        self.is_syncing = is_syncing

    @classmethod
    def from_line(cls, line):
        user_id, state = line.split(" ", 1)

        if state not in ("start", "end"):
            raise Exception("Invalid USER_SYNC state %r" % (state,))

        return cls(user_id, state == "start")

    def to_line(self):
        return " ".join((self.user_id, "start" if self.is_syncing else "end"))


class FederationAckCommand(Command):
    NAME = "FEDERATION_ACK"

    def __init__(self, token):
        self.token = token

    @classmethod
    def from_line(cls, line):
        return cls(int(line))

    def to_line(self):
        return str(self.token)


COMMAND_MAP = {
    cmd.NAME: cmd
    for cmd in (
        ServerCommand,
        RdataCommand,
        PositionCommand,
        ErrorCommand,
        PingCommand,
        NameCommand,
        ReplicateCommand,
        UserSyncCommand,
        FederationAckCommand,
    )
}

VALID_SERVER_COMMANDS = (
    ServerCommand.NAME,
    RdataCommand.NAME,
    PositionCommand.NAME,
    ErrorCommand.NAME,
    PingCommand.NAME,
)

VALID_CLIENT_COMMANDS = (
    NameCommand.NAME,
    ReplicateCommand.NAME,
    PingCommand.NAME,
    UserSyncCommand.NAME,
    FederationAckCommand.NAME,
)
