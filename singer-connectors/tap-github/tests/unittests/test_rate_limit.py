import tap_github.__init__ as tap_github
import unittest
from unittest import mock
import time
import requests
import importlib
import datetime
from email.utils import formatdate

def api_call():
    return requests.get("https://api.github.com/rate_limit")

@mock.patch('time.sleep')
class TestRateLimit(unittest.TestCase):

    def setUp(self) -> None:
        importlib.reload(tap_github)


    def test_rate_limit_wait_with_default_max_rate_limit(self, mocked_sleep):

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = str(int(round(time.time(), 0)) + 120)
        resp.headers["X-RateLimit-Remaining"] = "0"

        tap_github.rate_throttling(resp)

        mocked_sleep.assert_called_with(120 + tap_github.RATE_THROTTLING_EXTRA_WAITING_TIME)
        self.assertTrue(mocked_sleep.called)



    def test_rate_limit_exception_when_exceed_default_max_rate_limit(self, mocked_sleep):

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = str(int(round(time.time(), 0)) + 601)
        resp.headers["X-RateLimit-Remaining"] = '0'

        with self.assertRaises(tap_github.RateLimitExceeded):
            tap_github.rate_throttling(resp)


    def test_rate_limit_not_exceed_default_max_rate_limit(self, mocked_sleep):

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = str(int(round(time.time(), 0)) + 10)
        resp.headers["X-RateLimit-Remaining"] = '5'

        tap_github.rate_throttling(resp)

        self.assertFalse(mocked_sleep.called)

    def test_rate_limit_config_override_throw_exception(self, mocked_sleep):
        tap_github.MAX_RATE_LIMIT_WAIT_SECONDS = 1

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = str(int(round(time.time(), 0)) + 10)
        resp.headers["X-RateLimit-Remaining"] = "0"

        with self.assertRaises(tap_github.RateLimitExceeded):
            self.assertEqual(0, mocked_sleep.call_count)
            tap_github.rate_throttling(resp)

    @mock.patch.object(requests.Session, 'request')
    @mock.patch('tap_github.__init__.rate_throttling')
    def test_authed_get(self, mocked_rate_throttling, mocked_request, mocked_sleep):
        response = requests.Response()
        response.status_code = 200
        response._content = 'foo'
        mocked_request.return_value = response

        # Throttling should be called by default
        tap_github.authed_get('verifying repository access', 'https://api.github.com/repos/foo/commits')
        self.assertEqual(1, mocked_rate_throttling.call_count)

        # Throttling should be called if enabled explicitly
        mocked_rate_throttling.reset_mock()
        tap_github.authed_get('verifying repository access', 'https://api.github.com/repos/foo/commits',
                              do_rate_throttling=True)
        self.assertEqual(1, mocked_rate_throttling.call_count)

        # Throttling should not be called if it's disabled
        mocked_rate_throttling.reset_mock()
        tap_github.authed_get('verifying repository access', 'https://api.github.com/repos/foo/commits',
                              do_rate_throttling=False)
        self.assertEqual(0, mocked_rate_throttling.call_count)

    def test_retry_after_header_with_valid_value(self, mocked_sleep):
        mocked_sleep.side_effect = None

        retry_after_seconds = 30
        resp = api_call()
        resp.headers["Retry-After"] = str(retry_after_seconds)

        result = tap_github.rate_throttling(resp)

        mocked_sleep.assert_called_with(retry_after_seconds + tap_github.RATE_THROTTLING_EXTRA_WAITING_TIME)
        self.assertTrue(result)
        self.assertTrue(mocked_sleep.called)

    def test_retry_after_header_with_zero_value(self, mocked_sleep):
        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["Retry-After"] = "0"

        result = tap_github.rate_throttling(resp)

        self.assertFalse(result)
        self.assertFalse(mocked_sleep.called)

    def test_retry_after_header_exceeds_max_wait(self, mocked_sleep):
        mocked_sleep.side_effect = None

        retry_after_seconds = 700
        resp = api_call()
        resp.headers["Retry-After"] = str(retry_after_seconds)

        with self.assertRaises(tap_github.RateLimitExceeded):
            tap_github.rate_throttling(resp)

        self.assertFalse(mocked_sleep.called)

    def test_retry_after_takes_precedence_over_x_rate_limit(self, mocked_sleep):
        mocked_sleep.side_effect = None

        retry_after_seconds = 30
        resp = api_call()
        resp.headers["Retry-After"] = str(retry_after_seconds)
        resp.headers["X-RateLimit-Remaining"] = "0"
        resp.headers["X-RateLimit-Reset"] = str(int(round(time.time(), 0)) + 120)

        result = tap_github.rate_throttling(resp)

        mocked_sleep.assert_called_with(retry_after_seconds + tap_github.RATE_THROTTLING_EXTRA_WAITING_TIME)
        self.assertTrue(result)

    def test_rate_throttling_returns_true_when_sleeping(self, mocked_sleep):
        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = str(int(round(time.time(), 0)) + 120)
        resp.headers["X-RateLimit-Remaining"] = "0"

        result = tap_github.rate_throttling(resp)

        self.assertTrue(result)

    def test_rate_throttling_returns_false_when_not_sleeping(self, mocked_sleep):
        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Remaining"] = "100"

        result = tap_github.rate_throttling(resp)

        self.assertFalse(result)

    @mock.patch.object(requests.Session, 'request')
    def test_authed_get_retries_on_403_with_rate_limit(self, mocked_request, mocked_sleep):
        reset_seconds = 10
        response_403 = requests.Response()
        response_403.status_code = 403
        response_403._content = b'{"message": "API rate limit exceeded"}'
        response_403.headers["X-RateLimit-Remaining"] = "0"
        response_403.headers["X-RateLimit-Reset"] = str(int(round(time.time(), 0)) + reset_seconds)

        response_200 = requests.Response()
        response_200.status_code = 200
        response_200._content = b'{"data": "success"}'

        mocked_request.side_effect = [response_403, response_200]

        result = tap_github.authed_get('test', 'https://api.github.com/repos/foo/commits')

        self.assertEqual(2, mocked_request.call_count)
        self.assertEqual(200, result.status_code)
        self.assertTrue(mocked_sleep.called)

    @mock.patch.object(requests.Session, 'request')
    def test_authed_get_raises_on_403_without_rate_limit(self, mocked_request, mocked_sleep):
        remaining_requests = 100
        response_403 = requests.Response()
        response_403.status_code = 403
        response_403._content = b'{"message": "Forbidden"}'
        response_403.headers["X-RateLimit-Remaining"] = str(remaining_requests)

        mocked_request.return_value = response_403

        with self.assertRaises(tap_github.AuthException):
            tap_github.authed_get('test', 'https://api.github.com/repos/foo/commits')

        self.assertEqual(1, mocked_request.call_count)
        self.assertFalse(mocked_sleep.called)

    @mock.patch.object(requests.Session, 'request')
    def test_authed_get_retries_on_403_with_retry_after(self, mocked_request, mocked_sleep):
        retry_after_seconds = 5
        response_403 = requests.Response()
        response_403.status_code = 403
        response_403._content = b'{"message": "API rate limit exceeded"}'
        response_403.headers["Retry-After"] = str(retry_after_seconds)

        response_200 = requests.Response()
        response_200.status_code = 200
        response_200._content = b'{"data": "success"}'

        mocked_request.side_effect = [response_403, response_200]

        result = tap_github.authed_get('test', 'https://api.github.com/repos/foo/commits')

        self.assertEqual(2, mocked_request.call_count)
        self.assertEqual(200, result.status_code)
        mocked_sleep.assert_called_with(retry_after_seconds + tap_github.RATE_THROTTLING_EXTRA_WAITING_TIME)

    @mock.patch.object(requests.Session, 'request')
    def test_authed_get_stops_after_max_retries(self, mocked_request, mocked_sleep):
        response_403 = requests.Response()
        response_403.status_code = 403
        response_403._content = b'{"message": "API rate limit exceeded"}'
        response_403.headers["X-RateLimit-Remaining"] = "0"
        response_403.headers["X-RateLimit-Reset"] = str(int(round(time.time(), 0)) + 10)

        mocked_request.return_value = response_403

        with self.assertRaises(tap_github.AuthException):
            tap_github.authed_get('test', 'https://api.github.com/repos/foo/commits')

        self.assertEqual(4, mocked_request.call_count)
        self.assertEqual(3, mocked_sleep.call_count)

    @mock.patch('tap_github.__init__.datetime')
    def test_retry_after_header_with_http_date_format(self, mocked_datetime, mocked_sleep):
        mocked_sleep.side_effect = None

        fixed_now = datetime.datetime(2026, 1, 23, 12, 0, 0, tzinfo=datetime.timezone.utc)
        mocked_datetime.datetime.now.return_value = fixed_now
        mocked_datetime.timezone = datetime.timezone

        retry_after_seconds = 45
        future_time = fixed_now + datetime.timedelta(seconds=retry_after_seconds)
        http_date = formatdate(timeval=future_time.timestamp(), usegmt=True)

        resp = api_call()
        resp.headers["Retry-After"] = http_date

        result = tap_github.rate_throttling(resp)

        expected_sleep = retry_after_seconds + tap_github.RATE_THROTTLING_EXTRA_WAITING_TIME
        mocked_sleep.assert_called_with(expected_sleep)
        self.assertTrue(result)

    def test_retry_after_header_http_date_exceeds_max_wait(self, mocked_sleep):
        mocked_sleep.side_effect = None

        future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=700)
        http_date = formatdate(timeval=future_time.timestamp(), usegmt=True)

        resp = api_call()
        resp.headers["Retry-After"] = http_date

        with self.assertRaises(tap_github.RateLimitExceeded):
            tap_github.rate_throttling(resp)

        self.assertFalse(mocked_sleep.called)

    def test_retry_after_header_integer_format(self, mocked_sleep):
        mocked_sleep.side_effect = None

        retry_after_seconds = 25
        resp = api_call()
        resp.headers["Retry-After"] = str(retry_after_seconds)

        result = tap_github.rate_throttling(resp)

        mocked_sleep.assert_called_with(retry_after_seconds + tap_github.RATE_THROTTLING_EXTRA_WAITING_TIME)
        self.assertTrue(result)

    @mock.patch.object(requests.Session, 'request')
    def test_authed_get_retries_with_http_date_retry_after(self, mocked_request, mocked_sleep):
        future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=10)
        http_date = formatdate(timeval=future_time.timestamp(), usegmt=True)

        response_403 = requests.Response()
        response_403.status_code = 403
        response_403._content = b'{"message": "API rate limit exceeded"}'
        response_403.headers["Retry-After"] = http_date

        response_200 = requests.Response()
        response_200.status_code = 200
        response_200._content = b'{"data": "success"}'

        mocked_request.side_effect = [response_403, response_200]

        result = tap_github.authed_get('test', 'https://api.github.com/repos/foo/commits')

        self.assertEqual(2, mocked_request.call_count)
        self.assertEqual(200, result.status_code)
        self.assertTrue(mocked_sleep.called)
