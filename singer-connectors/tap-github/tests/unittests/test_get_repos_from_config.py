import unittest
from unittest import mock

import tap_github.__init__ as tap_github


class TestGetReposFromConfig(unittest.TestCase):

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_should_get_all_repos_from_org(self, mocked_get_all_repositories):
        repos = [
            {'id': 1, 'name': 'repo1', 'full_name': 'org/repo1', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 2, 'name': 'repo2', 'full_name': 'org/repo2', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 3, 'name': 'repo3', 'full_name': 'org/repo3', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'}
        ]
        mocked_get_all_repositories.return_value = repos
        config = {
            "organization": "org"
        }

        repositories = tap_github.get_repos_from_config(config)

        self.assertEqual(mocked_get_all_repositories.call_count, 1)
        self.assertEqual(3, len(repositories))
        self.assertEqual('org/repo1', repositories[0])
        self.assertEqual('org/repo2', repositories[1])
        self.assertEqual('org/repo3', repositories[2])

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_should_get_repos_from_org_with_include(self, mocked_get_all_repositories):
        repos = [
            {'id': 1, 'name': 'repo1', 'full_name': 'org/repo1', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 2, 'name': 'repo2', 'full_name': 'org/repo2', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 3, 'name': 'repo3', 'full_name': 'org/repo3', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'}
        ]
        mocked_get_all_repositories.return_value = repos
        config = {
            "organization": "org",
            "repos_include": "repo1 repo2 repo3"
        }

        repositories = tap_github.get_repos_from_config(config)

        self.assertEqual(mocked_get_all_repositories.call_count, 1)
        self.assertEqual(3, len(repositories))
        self.assertEqual('org/repo1', repositories[0])
        self.assertEqual('org/repo2', repositories[1])
        self.assertEqual('org/repo3', repositories[2])

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_should_get_all_repos_from_only_include(self, mocked_get_all_repositories):
        config = {
            "repos_include": "org/repo1 org/repo2 org/repo3"
        }

        repositories = tap_github.get_repos_from_config(config)

        self.assertEqual(mocked_get_all_repositories.call_count, 0)
        self.assertEqual(3, len(repositories))
        self.assertEqual('org/repo1', repositories[0])
        self.assertEqual('org/repo2', repositories[1])
        self.assertEqual('org/repo3', repositories[2])

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_should_get_repos_merging_include_and_repository(self, mocked_get_all_repositories):
        config = {
            "repos_include": "org/repo1 org/repo2",
            "repository": "org/repo3"
        }

        repositories = tap_github.get_repos_from_config(config)

        self.assertEqual(mocked_get_all_repositories.call_count, 0)
        self.assertEqual(3, len(repositories))
        self.assertEqual('org/repo1', repositories[0])
        self.assertEqual('org/repo2', repositories[1])
        self.assertEqual('org/repo3', repositories[2])

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_should_get_all_repos_from_only_deprecated_repository(self, mocked_get_all_repositories):
        config = {
            "repository": "org/repo1 org/repo2 org/repo3"
        }

        repositories = tap_github.get_repos_from_config(config)

        self.assertEqual(mocked_get_all_repositories.call_count, 0)
        self.assertEqual(len(repositories), 3)
        self.assertEqual('org/repo1', repositories[0])
        self.assertEqual('org/repo2', repositories[1])
        self.assertEqual('org/repo3', repositories[2])

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_org_required_with_exclude(self, mocked_get_all_repositories):
        mocked_get_all_repositories.return_value = []
        with self.assertRaises(tap_github.InvalidParametersException):
            tap_github.get_repos_from_config({"repos_exclude": "repo1"})
            self.assertEqual(0, mocked_get_all_repositories.call_count)

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_org_required_when_include_not_present(self, mocked_get_all_repositories):
        mocked_get_all_repositories.return_value = []

        with self.assertRaises(tap_github.InvalidParametersException):
            tap_github.get_repos_from_config({})
            self.assertEqual(0, mocked_get_all_repositories.call_count)

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_org_required_when_include_has_wildcard_matchers(self, mocked_get_all_repositories):
        mocked_get_all_repositories.return_value = []

        with self.assertRaises(tap_github.InvalidParametersException):
            tap_github.get_repos_from_config({
                "repos_include": "name*"
            })
            self.assertEqual(0, mocked_get_all_repositories.call_count)

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_org_required_when_exclude_has_wildcard_matchers(self, mocked_get_all_repositories):
        mocked_get_all_repositories.return_value = []

        with self.assertRaises(tap_github.InvalidParametersException):
            tap_github.get_repos_from_config({
                "repos_exclude": "name*"
            })
            self.assertEqual(0, mocked_get_all_repositories.call_count)

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_org_prefix_not_allowed_in_repos_include_when_organization_is_present(self, mocked_get_all_repositories):
        mocked_get_all_repositories.return_value = []

        with self.assertRaises(tap_github.InvalidParametersException):
            tap_github.get_repos_from_config({
                "organization": "org",
                "repos_include": "repo org2/repo1"
            })
            self.assertEqual(0, mocked_get_all_repositories.call_count)

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_should_fail_if_exclude_all(self, mocked_get_all_repositories):
        mocked_get_all_repositories.return_value = []

        with self.assertRaises(tap_github.InvalidParametersException):
            tap_github.get_repos_from_config({
                "organization": "org",
                "repos_exclude": "* repo1*"
            })
            self.assertEqual(0, mocked_get_all_repositories.call_count)

    @mock.patch("tap_github.__init__.get_all_repositories")
    def test_org_prefix_not_allowed_in_repos_exclude(self, mocked_get_all_repositories):
        mocked_get_all_repositories.return_value = []

        with self.assertRaises(tap_github.InvalidParametersException):
            tap_github.get_repos_from_config({
                "organization": "org",
                "repos_exclude": "repo1 org/repo"
            })
            self.assertEqual(0, mocked_get_all_repositories.call_count)
