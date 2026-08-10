from unittest import TestCase

from tests.end_to_end.helpers import tasks


class EndToEndTaskHelpersTestCase(TestCase):
    """Tests for command-output parsing used by the E2E assertions."""

    def test_find_run_tap_log_file_returns_the_only_matching_engine_log(self):
        """Return the one log belonging to the requested engine."""
        stdout = (
            'Writing output into /tmp/run.singer.log\n'
            'Writing output into /tmp/run.fastsync.log\n'
        )

        self.assertEqual(
            tasks.find_run_tap_log_file(stdout, 'fastsync'),
            '/tmp/run.fastsync.log',
        )

    def test_find_run_tap_log_file_rejects_missing_engine_log(self):
        """Fail clearly when the requested engine did not start."""
        with self.assertRaisesRegex(
            AssertionError,
            'Expected exactly one fastsync log file, found 0',
        ):
            tasks.find_run_tap_log_file('', 'fastsync')

    def test_find_run_tap_log_file_rejects_duplicate_engine_logs(self):
        """Reject an unintended second run of the same sync engine."""
        stdout = (
            'Writing output into /tmp/first.fastsync.log\n'
            'Writing output into /tmp/second.fastsync.log\n'
        )

        with self.assertRaisesRegex(
            AssertionError,
            'Expected exactly one fastsync log file, found 2',
        ):
            tasks.find_run_tap_log_file(stdout, 'fastsync')

    def test_log_engines_reject_extra(self):
        """Reject an unrequested different sync engine."""
        stdout = (
            'Writing output into /tmp/run.fastsync.log\n'
            'Writing output into /tmp/run.partialsync.log\n'
        )

        with self.assertRaisesRegex(
            AssertionError,
            "Expected sync engine logs {'fastsync': 1}, "
            "found {'fastsync': 1, 'partialsync': 1}",
        ):
            tasks.assert_run_tap_log_engines(stdout, ('fastsync',))

    def test_log_engines_accept_exact_set(self):
        """Accept one log for every requested engine and no others."""
        stdout = (
            'Writing output into /tmp/run.fastsync.log\n'
            'Writing output into /tmp/run.singer.log\n'
        )

        tasks.assert_run_tap_log_engines(stdout, ('fastsync', 'singer'))
