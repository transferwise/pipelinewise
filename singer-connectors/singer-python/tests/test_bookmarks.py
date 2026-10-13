import unittest
from singer import bookmarks


class TestGetBookmark(unittest.TestCase):
    def test_empty_state(self):
        empty_state = {}

        # Case with no value to fall back on
        self.assertIsNone(bookmarks.get_bookmark(empty_state, 'some_stream', 'my_key'))

        # Case with a given default
        self.assertEqual(bookmarks.get_bookmark(empty_state, 'some_stream', 'my_key', 'default_value'),
                         'default_value')

    def test_empty_bookmark(self):
        empty_bookmark = {'bookmarks':{}}

        # Case with no value to fall back on
        self.assertIsNone(bookmarks.get_bookmark(empty_bookmark, 'some_stream', 'my_key'))

        # Case with a given default
        self.assertEqual(bookmarks.get_bookmark(empty_bookmark, 'some_stream', 'my_key', 'default_value'),
                         'default_value')

    def test_non_empty_state(self):
        stream_id_1 = 'customers'
        bookmark_key_1 = 'datetime'
        bookmark_val_1 = 123456789

        non_empty_state = {
            'bookmarks' : {
                stream_id_1 : {
                    bookmark_key_1 : bookmark_val_1
                }
            }
        }

        #
        # Cases with no value to fall back on
        #

        # Bad stream, bad key
        self.assertIsNone(bookmarks.get_bookmark(non_empty_state, 'some_stream', 'my_key'))

        # Good stream, bad key
        self.assertIsNone(bookmarks.get_bookmark(non_empty_state, stream_id_1, 'my_key'))

        # Good stream, good key
        self.assertEqual(bookmarks.get_bookmark(non_empty_state, stream_id_1, bookmark_key_1),
                         bookmark_val_1)

        #
        # Cases with a given default
        #

        # Bad stream, bad key
        self.assertEqual(bookmarks.get_bookmark(non_empty_state, 'some_stream', 'my_key', 'default_value'),
                         'default_value')

        # Bad stream, good key
        self.assertEqual(bookmarks.get_bookmark(non_empty_state, 'some_stream', bookmark_key_1, 'default_value'),
                         'default_value')

        # Good stream, bad key
        self.assertEqual(bookmarks.get_bookmark(non_empty_state, stream_id_1, 'my_key', 'default_value'),
                         'default_value')

        # Good stream, good key
        self.assertEqual(bookmarks.get_bookmark(non_empty_state, stream_id_1, bookmark_key_1, 'default_value'),
                         bookmark_val_1)


class TestGetOffset(unittest.TestCase):
    def test_empty_state(self):
        empty_state = {}

        # Case with no value to fall back on
        self.assertIsNone(bookmarks.get_offset(empty_state, 'some_stream'))

        # Case with a given default
        self.assertEqual(bookmarks.get_offset(empty_state, 'some_stream', 'default_value'),
                         'default_value')

    def test_empty_bookmark(self):
        empty_bookmark = {'bookmarks':{}}

        # Case with no value to fall back on
        self.assertIsNone(bookmarks.get_offset(empty_bookmark, 'some_stream'))

        # Case with a given default
        self.assertEqual(bookmarks.get_offset(empty_bookmark, 'some_stream', 'default_value'),
                         'default_value')

    def test_non_empty_state(self):
        stream_id_1 = 'customers'
        bookmark_key_1 = 'datetime'
        bookmark_val_1 = 123456789
        offset_val = 'fizzy water'

        non_empty_state = {
            'bookmarks' : {
                stream_id_1 : {
                    bookmark_key_1 : bookmark_val_1,
                    'offset' : offset_val
                }
            }
        }

        #
        # Cases with no value to fall back on
        #

        # Bad stream
        self.assertIsNone(bookmarks.get_offset(non_empty_state, 'some_stream'))

        # Good stream
        self.assertEqual(bookmarks.get_offset(non_empty_state, stream_id_1),
                         offset_val)

        #
        # Case with a given default
        #

        # Bad stream
        self.assertEqual(bookmarks.get_offset(non_empty_state, 'some_stream', 'default_value'),
                         'default_value')

        # Good stream
        self.assertEqual(bookmarks.get_offset(non_empty_state, stream_id_1, 'default_value'),
                         offset_val)


class TestGetCurrentlySyncing(unittest.TestCase):
    def test_empty_state(self):
        empty_state = {}

        # Case with no value to fall back on
        self.assertIsNone(bookmarks.get_currently_syncing(empty_state))

        # Case with a given default
        self.assertEqual(bookmarks.get_currently_syncing(empty_state, 'default_value'),
                         'default_value')

    def test_non_empty_state(self):
        stream_id_1 = 'customers'
        bookmark_key_1 = 'datetime'
        bookmark_val_1 = 123456789
        offset_val = 'fizzy water'

        non_empty_state = {
            'bookmarks' : {
                stream_id_1 : {
                    bookmark_key_1 : bookmark_val_1,
                    'offset' : offset_val
                }
            },
            'currently_syncing' : stream_id_1
        }

        # Case with no value to fall back on
        self.assertEqual(bookmarks.get_currently_syncing(non_empty_state),
                         stream_id_1)

        # Case with a given default
        self.assertEqual(bookmarks.get_currently_syncing(non_empty_state, 'default_value'),
                         stream_id_1)
