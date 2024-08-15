import unittest
import singer
import singer.metadata
import tap_mysql

from tap_mysql.connection import connect_with_backoff

try:
    import tests.integration.utils as test_utils
except ImportError:
    import utils as test_utils

SINGER_MESSAGES = []
TABLE_2_RECORD_COUNT = 0

#                FOO   BAR
TABLE_1_DATA = [[100, 'abc'],
                [200, 'def'],
                [300, 'ghi']]

TABLE_2_DATA = TABLE_1_DATA[::-1]


def insert_record(conn, table_name, record):
    value_sql = ",".join(["%s" for i in range(len(record))])

    insert_sql = """
        INSERT INTO {}.{}
               ( `foo`, `bar` )
        VALUES ( {} )""".format(
        test_utils.DB_NAME,
        table_name,
        value_sql)

    with connect_with_backoff(conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute(insert_sql, record)


def singer_write_message_no_table_2(message):
    global TABLE_2_RECORD_COUNT

    if isinstance(message, singer.RecordMessage) and message.stream == 'tap_mysql_test-table_2':
        TABLE_2_RECORD_COUNT = TABLE_2_RECORD_COUNT + 1

        if TABLE_2_RECORD_COUNT > 1:
            raise Exception("simulated exception")

    SINGER_MESSAGES.append(message)


def singer_write_message_ok(message):
    SINGER_MESSAGES.append(message)


def init_tables(conn):
    with connect_with_backoff(conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE table_1 (
            id  BIGINT AUTO_INCREMENT PRIMARY KEY,
            foo BIGINT,
            bar VARCHAR(10)
            )""")

            cur.execute("""
            CREATE TABLE table_2 (
            id  BIGINT AUTO_INCREMENT PRIMARY KEY,
            foo BIGINT,
            bar VARCHAR(10)
            )""")

    for record in TABLE_1_DATA:
        insert_record(conn, 'table_1', record)

    for record in TABLE_2_DATA:
        insert_record(conn, 'table_2', record)

    catalog = test_utils.discover_catalog(conn, {})

    return catalog


class BinlogInterruption(unittest.TestCase):
    def setUp(self):
        self.conn = test_utils.get_test_connection()
        self.catalog = init_tables(self.conn)

        for stream in self.catalog.streams:
            stream.metadata = [
                {'breadcrumb': (),
                 'metadata': {'selected': True,
                              'database-name': 'tap_mysql_test',
                              'table-key-properties': ['id']}},
                {'breadcrumb': ('properties', 'id'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'foo'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'bar'), 'metadata': {'selected': True}},
            ]

            stream.stream = stream.table

            if stream.table == 'table_2':
                test_utils.set_replication_method_and_key(stream, 'LOG_BASED', None)
            else:
                test_utils.set_replication_method_and_key(stream, 'FULL_TABLE', None)

        global TABLE_2_RECORD_COUNT
        TABLE_2_RECORD_COUNT = 0

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

    def test_table_2_interrupted(self):
        singer.write_message = singer_write_message_no_table_2

        state = {}
        failed_syncing_table_2 = False

        try:
            tap_mysql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)
        except Exception as ex:
            if str(ex) == 'simulated exception':
                failed_syncing_table_2 = True

        self.assertTrue(failed_syncing_table_2)

        record_messages_1 = [[m.stream, m.record] for m in SINGER_MESSAGES
                             if isinstance(m, singer.RecordMessage)]

        self.assertEqual(record_messages_1,
                         [['tap_mysql_test-table_1', {'id': 1, 'bar': 'abc', 'foo': 100}],
                          ['tap_mysql_test-table_1', {'id': 2, 'bar': 'def', 'foo': 200}],
                          ['tap_mysql_test-table_1', {'id': 3, 'bar': 'ghi', 'foo': 300}],
                          ['tap_mysql_test-table_2', {'id': 1, 'bar': 'ghi', 'foo': 300}]])

        self.assertEqual(state['currently_syncing'], 'tap_mysql_test-table_2')

        table_1_bookmark = state['bookmarks']['tap_mysql_test-table_1']
        table_2_bookmark = state['bookmarks']['tap_mysql_test-table_2']

        self.assertEqual(table_1_bookmark,
                         {'initial_full_table_complete': True})

        self.assertIsNone(table_2_bookmark.get('initial_full_table_complete'))

        table_2_version = table_2_bookmark['version']
        self.assertIsNotNone(table_2_version)

        self.assertEqual(table_2_bookmark['max_pk_values'], {'id': 3})
        self.assertEqual(table_2_bookmark['last_pk_fetched'], {'id': 1})

        self.assertIsNotNone(table_2_bookmark.get('log_file'))
        self.assertIsNotNone(table_2_bookmark.get('log_pos'))

        failed_syncing_table_2 = False
        singer.write_message = singer_write_message_ok

        SINGER_MESSAGES.clear()

        tap_mysql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)

        self.assertFalse(failed_syncing_table_2)

        record_messages_2 = [[m.stream, m.record] for m in SINGER_MESSAGES
                             if isinstance(m, singer.RecordMessage)]

        self.assertEqual(record_messages_2,
                         [['tap_mysql_test-table_2', {'id': 2, 'bar': 'def', 'foo': 200}],
                          ['tap_mysql_test-table_2', {'id': 3, 'bar': 'abc', 'foo': 100}],
                          ['tap_mysql_test-table_1', {'id': 1, 'bar': 'abc', 'foo': 100}],
                          ['tap_mysql_test-table_1', {'id': 2, 'bar': 'def', 'foo': 200}],
                          ['tap_mysql_test-table_1', {'id': 3, 'bar': 'ghi', 'foo': 300}]])

        self.assertIsNone(state['currently_syncing'])

        table_1_bookmark = state['bookmarks']['tap_mysql_test-table_1']
        table_2_bookmark = state['bookmarks']['tap_mysql_test-table_2']

        self.assertEqual(table_1_bookmark,
                         {'initial_full_table_complete': True})

        self.assertIsNone(table_2_bookmark.get('initial_full_table_complete'))

        table_2_version = table_2_bookmark['version']
        self.assertIsNotNone(table_2_version)

        self.assertIsNone(table_2_bookmark.get('max_pk_values'))
        self.assertIsNone(table_2_bookmark.get('last_pk_fetched'))

        self.assertIsNotNone(table_2_bookmark.get('log_file'))
        self.assertIsNotNone(table_2_bookmark.get('log_pos'))

        new_table_2_records = [[400, 'jkl'],
                               [500, 'mno']]

        for record in new_table_2_records:
            insert_record(self.conn, 'table_2', record)

        SINGER_MESSAGES.clear()

        tap_mysql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state)

        self.assertFalse(failed_syncing_table_2)

        record_messages_3 = [[m.stream, m.record] for m in SINGER_MESSAGES
                             if isinstance(m, singer.RecordMessage)]

        self.assertEqual(record_messages_3,
                         [['tap_mysql_test-table_1', {'id': 1, 'bar': 'abc', 'foo': 100}],
                          ['tap_mysql_test-table_1', {'id': 2, 'bar': 'def', 'foo': 200}],
                          ['tap_mysql_test-table_1', {'id': 3, 'bar': 'ghi', 'foo': 300}],
                          ['tap_mysql_test-table_2', {'id': 4, 'bar': 'jkl', 'foo': 400}],
                          ['tap_mysql_test-table_2', {'id': 5, 'bar': 'mno', 'foo': 500}]])

        self.assertIsNone(state['currently_syncing'])

        table_1_bookmark = state['bookmarks']['tap_mysql_test-table_1']
        table_2_bookmark = state['bookmarks']['tap_mysql_test-table_2']

        self.assertEqual(table_1_bookmark,
                         {'initial_full_table_complete': True})

        self.assertIsNone(table_2_bookmark.get('initial_full_table_complete'))
        self.assertIsNotNone(table_2_bookmark.get('log_file'))
        self.assertIsNotNone(table_2_bookmark.get('log_pos'))


class FullTableInterruption(unittest.TestCase):
    def setUp(self):
        self.conn = test_utils.get_test_connection()
        self.catalog = init_tables(self.conn)

        for stream in self.catalog.streams:
            stream.metadata = [
                {'breadcrumb': (),
                 'metadata': {'selected': True,
                              'database-name': 'tap_mysql_test',
                              'table-key-properties': ['id']}},
                {'breadcrumb': ('properties', 'id'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'foo'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'bar'), 'metadata': {'selected': True}},
            ]

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, 'FULL_TABLE', None)

        global TABLE_2_RECORD_COUNT
        TABLE_2_RECORD_COUNT = 0

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

    def test_table_2_interrupted(self):
        singer.write_message = singer_write_message_no_table_2

        state = {}
        failed_syncing_table_2 = False

        try:
            tap_mysql.do_sync(self.conn, {}, self.catalog, state)
        except Exception as ex:
            if str(ex) == 'simulated exception':
                failed_syncing_table_2 = True

        self.assertTrue(failed_syncing_table_2)

        record_messages_1 = [[m.stream, m.record] for m in SINGER_MESSAGES
                             if isinstance(m, singer.RecordMessage)]

        self.assertEqual(record_messages_1,
                         [['tap_mysql_test-table_1', {'id': 1, 'bar': 'abc', 'foo': 100}],
                          ['tap_mysql_test-table_1', {'id': 2, 'bar': 'def', 'foo': 200}],
                          ['tap_mysql_test-table_1', {'id': 3, 'bar': 'ghi', 'foo': 300}],
                          ['tap_mysql_test-table_2', {'id': 1, 'bar': 'ghi', 'foo': 300}]
                          ])

        failed_syncing_table_2 = False
        singer.write_message = singer_write_message_ok

        SINGER_MESSAGES.clear()

        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        self.assertFalse(failed_syncing_table_2)

        record_messages_2 = [[m.stream, m.record] for m in SINGER_MESSAGES if isinstance(m, singer.RecordMessage)]
        self.assertEqual(record_messages_2,
                         [['tap_mysql_test-table_2', {'id': 2, 'bar': 'def', 'foo': 200}],
                          ['tap_mysql_test-table_2', {'id': 3, 'bar': 'abc', 'foo': 100}],
                          ['tap_mysql_test-table_1', {'id': 1, 'bar': 'abc', 'foo': 100}],
                          ['tap_mysql_test-table_1', {'id': 2, 'bar': 'def', 'foo': 200}],
                          ['tap_mysql_test-table_1', {'id': 3, 'bar': 'ghi', 'foo': 300}]])

        expected_state_2 = {
            'currently_syncing': None,
            'bookmarks': {
                'tap_mysql_test-table_1': {
                    'initial_full_table_complete': True
                },
                'tap_mysql_test-table_2': {
                    'initial_full_table_complete': True
                }
            }
        }

        self.assertEqual(state, expected_state_2)


if __name__ == "__main__":
    test1 = BinlogInterruption()
    test1.setUp()
    test1.test_table_2_interrupted()
