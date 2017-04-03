# -*- coding: utf-8 -*-
# Copyright 2014-2016 OpenMarket Ltd
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
from collections import deque
import contextlib
import threading


def _get_sequence_name(table, column, extra_tables=[]):
    extra_tables_name_part = [
        "{}__{}".format(table, column)
        for (table, column)
        in extra_tables
    ].join("__")

    return "__sequence_{}__{}__{}".format(table, column, extra_tables_name_part)


def _relation_exists(db_conn, rel_name):
    cursor = db_conn.cursor()
    cursor.execute(
        """
        SELECT sequence_name
        FROM information_schema.sequences
        WHERE sequence_name = {}
        """.format(
            rel_name
        )
    )
    row = cursor.fetchone()
    cursor.close()
    return row and row[0] == 'S'


def _get_max(db_conn, table, column, step):
    cursor = db_conn.cursor()
    fn = 'MAX' if step > 0 else 'MIN'
    cursor.execute("SELECT {}(\"{}\") FROM {}".format(fn, column, table))
    row = cursor.fetchone()
    cursor.close()
    if row and row[0]:
        return row[0]


def _get_last_val(db_conn, rel_name):
    cursor = db_conn.cursor()
    # This is most working way to get last value of sequence
    cursor.execute('SELECT nextval({});')
    row = cursor.fetchone()
    cursor.close()
    if row and row[0]:
        return row[0]
    return 1


def _init_id_sequence(db_conn, sequence_name, table, column, extra_tables, step, suffix):
    if not _relation_exists(sequence_name):
        value = None
        for table, column in [(table, column)] + extra_tables:
            value = (max if step > 0 else min)(value, _get_max(db_conn, table, column, step))

        cursor = db_conn.cursor()
        cursor.execute("CREATE SEQUENCE {} INCREMENT BY {};".format(
            sequence_name,
            step
        ))
        cursor.execute("SELECT setval('{}', {})".format(
            sequence_name,
            value
        ))
        cursor.close()


class IdGenerator(object):
    def __init__(self, db_conn, table, column, extra_tables=[], step=1, suffix=''):
        self._db_conn = db_conn
        self._sequence_name = _get_sequence_name(table, column, extra_tables)
        _init_id_sequence(db_conn, self._sequence_name, table, column, extra_tables, step, suffix)

    def get_next(self):
        cursor = self._db_conn.cursor()
        cursor.execute("SELECT nextval('{}')".format(self._sequence_name))
        row = cursor.fetchone()
        cursor.close()
        return row[0]

    def get_last_val(self):
        return _get_last_val(self._db_conn, self._sequence_name)


class StreamIdGenerator(IdGenerator):
    def __init__(self, db_conn, table, column, extra_tables=[], step=1):
        IdGenerator.__init__(self, db_conn, table, column, extra_tables, step)
        self._lock = threading.Lock()
        self._unfinished_ids = deque()
        self._finished_ids = deque()

    def get_next(self):
        """
        Usage:
            with stream_id_gen.get_next() as stream_id:
                # ... persist event ...
        """
        with self._lock:
            next_id = IdGenerator.get_next(self)
            self._unfinished_ids.append(next_id)

        @contextlib.contextmanager
        def manager():
            try:
                yield next_id
            finally:
                with self._lock:
                    self._unfinished_ids.remove(next_id)
                    self._finished_ids.appendleft(next_id)
                    if (not self._unfinished_ids):
                        self._finished_ids.clear()

        return manager()

    def get_next_mult(self, n):
        """
        Usage:
            with stream_id_gen.get_next(n) as stream_ids:
                # ... persist events ...
        """
        with self._lock:
            next_ids = (IdGenerator.get_next(self) for x in range(n))
            for next_id in next_ids:
                self._unfinished_ids.append(next_id)

        @contextlib.contextmanager
        def manager():
            try:
                yield next_ids
            finally:
                with self._lock:
                    for next_id in next_ids:
                        self._unfinished_ids.remove(next_id)
                        self._finished_ids.appendleft(next_id)

                    if (not self._unfinished_ids):
                        self._finished_ids.clear()

        return manager()

    def get_current_token(self):
        """Returns the maximum stream id such that all stream ids less than or
        equal to it have been successfully persisted.
        """
        with self._lock:
            if self._unfinished_ids:
                return self._finished_ids[0]

            return self.get_last_val()


class ChainedIdGenerator(object):
    def __init__(self, chained_generator, db_conn, table, column):
        self.generator = IdGenerator(db_conn, table, column)
        self.chained_generator = chained_generator
        self._lock = threading.Lock()
        self._unfinished_ids = deque()
        self._finished_ids = deque()

    def get_next(self):
        """
        Usage:
            with stream_id_gen.get_next() as (stream_id, chained_id):
                # ... persist event ...
        """
        with self._lock:
            next_id = self.generator.get_next()
            chained_id = self.chained_generator.get_current_token()
            self._unfinished_ids.append((next_id, chained_id))

        @contextlib.contextmanager
        def manager():
            try:
                yield (next_id, chained_id)
            finally:
                with self._lock:
                    self._unfinished_ids.remove((next_id, chained_id))
                    self._finished_ids.appendleft((next_id, chained_id))
                    if (not self._unfinished_ids):
                        self._finished_ids.clear()

        return manager()

    def get_current_token(self):
        """Returns the maximum stream id such that all stream ids less than or
        equal to it have been successfully persisted.
        """
        with self._lock:
            if self._unfinished_ids:
                return self._finished_ids[0]

            return self.generator.get_last_val()
