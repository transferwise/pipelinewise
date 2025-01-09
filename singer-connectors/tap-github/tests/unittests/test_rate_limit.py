import tap_github.__init__ as tap_github
import unittest
from unittest import mock
import time
import requests
import importlib

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
