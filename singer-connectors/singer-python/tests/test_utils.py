import unittest
from datetime import datetime as dt
import pytz
import logging
import singer.utils as u
from unittest.mock import patch


class TestFormat(unittest.TestCase):
    def test_small_years(self):
        self.assertEqual(u.strftime(dt(90, 1, 1, tzinfo=pytz.UTC)),
                         '0090-01-01T00:00:00.000000Z')

    def test_round_trip(self):
        now = dt.utcnow().replace(tzinfo=pytz.UTC)
        dtime = u.strftime(now)
        pdtime = u.strptime_to_utc(dtime)
        fdtime = u.strftime(pdtime)
        self.assertEqual(dtime, fdtime)

    def test_parse_naive_datetime(self):
        """Test that a naive datetime string is assigned UTC."""
        dt_string = "2026-04-17 09:00:00"
        result = u.strptime_with_tz(dt_string)
        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.tzinfo, pytz.UTC)
        self.assertEqual(result.year, 2026)
        self.assertEqual(result.hour, 9)

    def test_parse_aware_datetime(self):
        """Test that an existing timezone is preserved (not overwritten by UTC)."""
        # Using an offset of +02:00
        dt_string = "2026-04-17 09:00:00+02:00"
        result = u.strptime_with_tz(dt_string)

        self.assertIsNotNone(result.tzinfo)
        # The offset should be 120 minutes (2 hours)
        self.assertEqual(result.utcoffset().total_seconds(), 7200)
        self.assertNotEqual(result.tzinfo, pytz.UTC)

    def test_parse_iso_format(self):
        """Test that standard ISO formats work correctly."""
        dt_string = "2026-04-17T12:30:45Z"
        result = u.strptime_with_tz(dt_string)

        self.assertEqual(result.hour, 12)
        # 'Z' is UTC
        self.assertEqual(result.tzinfo.utcoffset(result).total_seconds(), 0)

    def test_invalid_string_raises_error(self):
        """Test that completely invalid strings still raise parser errors."""
        with self.assertRaises(Exception):
            u.strptime_with_tz("not-a-date")

    @patch('time.time')
    @patch('time.sleep')
    def test_ratelimit_sleeps_when_limit_exceeded(self, mock_sleep, mock_time):
        """Verify that sleep is called correctly after the limit is reached."""
        # Setup: Limit of 2 calls every 10 seconds
        limit = 2
        every = 10

        @u.ratelimit(limit, every)
        def mock_func():
            return True

        # First call at T=0
        # Second call at T=1
        # Third call at T=2 (This should trigger a sleep)
        mock_time.side_effect = [0.0, 1.0, 2.0, 3.0]

        # Call 1: Success, no sleep
        mock_func()
        # Call 2: Success, no sleep
        mock_func()

        # Call 3: This exceeds the limit of 2.
        # It compares current time (2.0) with the oldest time (0.0).
        # elapsed = 2.0 - 0.0 = 2.0.
        # sleep_time = 10 - 2.0 = 8.0.
        mock_func()

        # Check if sleep was called with 8.0 seconds
        mock_sleep.assert_called_once_with(8.0)

    @patch('time.time')
    @patch('time.sleep')
    def test_ratelimit_no_sleep_if_enough_time_passed(self, mock_sleep, mock_time):
        """Verify no sleep occurs if the time elapsed exceeds the 'every' window."""
        limit = 2
        every = 10

        @u.ratelimit(limit, every)
        def mock_func():
            return True

        # Call 1 at T=0
        # Call 2 at T=1
        # Call 3 at T=15 (15s passed since Call 1, which is > 'every' window of 10s)
        mock_time.side_effect = [0.0, 1.0, 15.0, 16.0]

        mock_func()
        mock_func()
        mock_func()

        # Sleep should NOT have been called
        mock_sleep.assert_not_called()

    def test_decorator_preserves_metadata(self):
        """Ensure functools.wraps is working to keep function names."""

        @u.ratelimit(5, 60)
        def original_function():
            """Docstring"""
            pass

        self.assertEqual(original_function.__name__, "original_function")
        self.assertEqual(original_function.__doc__, "Docstring")


class TestHandleException(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger(__name__)

    def test_successful_fn(self):
        @u.handle_top_exception(self.logger)
        def foo():
            return 3
        self.assertEqual(foo(), 3)

    def test_exception_fn(self):
        @u.handle_top_exception(self.logger)
        def foo():
            raise RuntimeError('foo')
        self.assertRaises(RuntimeError, foo)
