import tap_github
import unittest
from unittest import mock

@mock.patch("singer.bookmarks.get_bookmark")
class TestBookmarkStartDate(unittest.TestCase):

    def test_no_bookmark_no_start_date(self, mocked_get_bookmark):
        # Start date is none and bookmark is not present then None should be return.
        mocked_get_bookmark.return_value = None
        start_date = None
        bookmark_key = 'since'
        expected_bookmark_value  = None

        self.assertEqual(expected_bookmark_value, tap_github.get_bookmark('', '', '', bookmark_key, start_date))

    def test_no_bookmark_yes_start_date(self, mocked_get_bookmark):
        # Start date is present and bookmark is not present then start date should be return.
        mocked_get_bookmark.return_value = None
        start_date = '2021-04-01T00:00:00.000000Z'
        bookmark_key = 'since'
        expected_bookmark_value  = '2021-04-01T00:00:00.000000Z'

        self.assertEqual(expected_bookmark_value, tap_github.get_bookmark('', '', '', bookmark_key, start_date))

    def test_yes_bookmark_yes_start_date(self, mocked_get_bookmark):
        # Start date and bookmark both are present then bookmark should be return.
        mocked_get_bookmark.return_value = {"since" : "2021-05-01T00:00:00.000000Z"}
        start_date = '2021-04-01T00:00:00.000000Z'
        bookmark_key = 'since'
        expected_bookmark_value  = '2021-05-01T00:00:00.000000Z'

        self.assertEqual(expected_bookmark_value, tap_github.get_bookmark('', '', '', bookmark_key, start_date))

    def test_yes_bookmark_no_start_date(self, mocked_get_bookmark):
        # Start date is not present and bookmark is present then bookmark should be return.
        mocked_get_bookmark.return_value = {"since" : "2021-05-01T00:00:00.000000Z"}
        start_date = None
        bookmark_key = 'since'
        expected_bookmark_value  = '2021-05-01T00:00:00.000000Z'

        self.assertEqual(expected_bookmark_value, tap_github.get_bookmark('', '', '', bookmark_key, start_date))
