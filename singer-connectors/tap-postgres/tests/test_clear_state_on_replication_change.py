import unittest
import tap_postgres

tap_stream_id = 'chicken_table'

class TestClearState(unittest.TestCase):

    def test_incremental_happy(self):
        state = {'bookmarks' : {tap_stream_id : { 'version' : 1, "replication_key" : 'updated_at', 'replication_key_value' : '2017-01-01T00:00:03+00:00', 'last_replication_method' : 'INCREMENTAL'}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, 'updated_at', 'INCREMENTAL')
        self.assertEqual(nascent_state, state)

    def test_incremental_changing_replication_keys(self):
        state = {'bookmarks' : {tap_stream_id : { 'version' : 1, "replication_key" : 'updated_at', 'replication_key_value' : '2017-01-01T00:00:03+00:00', 'last_replication_method' : 'INCREMENTAL'}}}

        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, 'updated_at_2', 'INCREMENTAL')
        self.assertEqual(nascent_state, {'bookmarks' : {tap_stream_id : {'last_replication_method' : 'INCREMENTAL'}}})

    def test_incremental_changing_replication_key_interrupted(self):
        xmin = '3737373'
        state = {'bookmarks' : {tap_stream_id : { 'version' : 1, 'xmin' : xmin, "replication_key" : 'updated_at', 'replication_key_value' : '2017-01-01T00:00:03+00:00',
                                                  'last_replication_method' : 'INCREMENTAL'}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, 'updated_at_2', 'INCREMENTAL')
        self.assertEqual(nascent_state, {'bookmarks' : {tap_stream_id : { 'last_replication_method' : 'INCREMENTAL'}}})

    def test_full_table_to_incremental(self):
        #interrupted full table -> incremental
        xmin = '3737373'
        state = {'bookmarks' : {tap_stream_id : { 'version' : 1, 'xmin' : xmin, "last_replication_method" : "FULL_TABLE"}}}

        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, 'updated_at', 'INCREMENTAL')
        self.assertEqual(nascent_state, {'bookmarks' : {tap_stream_id : {"last_replication_method" : "INCREMENTAL"}}})

        state = {'bookmarks' : {tap_stream_id : { 'version' : 1, "last_replication_method" : "FULL_TABLE"}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, 'updated_at', 'INCREMENTAL')
        self.assertEqual(nascent_state, {'bookmarks' : {tap_stream_id : {"last_replication_method" : "INCREMENTAL"}}})


    def test_log_based_to_incremental(self):
        state = {'bookmarks' : {tap_stream_id : { 'version' : 1, 'lsn' : 34343434, "last_replication_method" : "LOG_BASED"}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, 'updated_at', 'INCREMENTAL')
        self.assertEqual(nascent_state, {'bookmarks' : {tap_stream_id : {"last_replication_method" : "INCREMENTAL"}}})

        state = {'bookmarks' : {tap_stream_id : { 'version' : 1, 'lsn' : 34343434, 'xmin' : 34343, "last_replication_method" : "LOG_BASED"}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, 'updated_at', 'INCREMENTAL')
        self.assertEqual(nascent_state, {'bookmarks' : {tap_stream_id : {"last_replication_method" : "INCREMENTAL"}}})

    #full table tests
    def test_full_table_happy(self):
        state = {'bookmarks' : {tap_stream_id : { 'version' : 88, "last_replication_method" : "FULL_TABLE"}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, None, 'FULL_TABLE')
        self.assertEqual(nascent_state, state)

    def test_full_table_interrupted(self):
        xmin = 333333
        state = {'bookmarks' : {tap_stream_id : { 'version' : 88, "last_replication_method" : "FULL_TABLE", 'xmin' : xmin}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, None, 'FULL_TABLE')
        self.assertEqual(nascent_state, {'bookmarks' : {tap_stream_id : { "last_replication_method" : "FULL_TABLE", 'version': 88, 'xmin' : xmin}}})

    def test_incremental_to_full_table(self):
        state = {'bookmarks' : {tap_stream_id : { 'version' : 88, "last_replication_method" : "INCREMENTAL", 'replication_key' : 'updated_at', 'replication_key_value' : 'i will be removed'}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, None, 'FULL_TABLE')
        self.assertEqual(nascent_state, {'bookmarks' : {tap_stream_id : { "last_replication_method" : "FULL_TABLE"}}})

    def test_log_based_to_full_table(self):
        state = {'bookmarks' : {tap_stream_id : { 'version' : 88, "last_replication_method" : "LOG_BASED", 'lsn' : 343434}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, None, 'FULL_TABLE')
        self.assertEqual(nascent_state, {'bookmarks' : {tap_stream_id : { "last_replication_method" : "FULL_TABLE"}}})


    #log based tests
    def test_log_based_happy(self):
        lsn = 43434343
        state = {'bookmarks' : {tap_stream_id : { 'version' : 88, "last_replication_method" : "LOG_BASED", 'lsn' : lsn}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, None, 'LOG_BASED')
        self.assertEqual(nascent_state, state)

        lsn = 43434343
        xmin = 11111
        state = {'bookmarks' : {tap_stream_id : { 'version' : 88, "last_replication_method" : "LOG_BASED", 'lsn' : lsn, 'xmin' : xmin}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, None, 'LOG_BASED')
        self.assertEqual(nascent_state, state)

    def test_incremental_to_log_based(self):
        state = {'bookmarks' : {tap_stream_id : { 'version' : 88, "last_replication_method" : "INCREMENTAL", 'replication_key' : 'updated_at', 'replication_key_value' : 'i will be removed'}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, None, 'LOG_BASED')
        self.assertEqual(nascent_state, {'bookmarks' : {tap_stream_id : { "last_replication_method" : "LOG_BASED"}}})

    def test_full_table_to_log_based(self):
        state = {'bookmarks' : {tap_stream_id : { 'version' : 2222, "last_replication_method" : "FULL_TABLE", 'xmin' : 2}}}
        nascent_state = tap_postgres.clear_state_on_replication_change(state, tap_stream_id, None, 'LOG_BASED')
        self.assertEqual(nascent_state, {'bookmarks' : {tap_stream_id : { "last_replication_method" : "LOG_BASED"}}})



if __name__== "__main__":
    test1 = TestClearState()
    test1.test_full_table_to_log_based()
