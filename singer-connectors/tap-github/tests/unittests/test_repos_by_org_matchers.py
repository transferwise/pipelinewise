import unittest
from unittest import mock
from unittest.mock import Mock

import requests

import tap_github


def get_response(status_code=200, json=[], raise_error=False):
    response_mock = Mock(spec_set=requests.Response)
    response_mock.configure_mock(**{
        'raise_for_status.side_effect': status_code if not raise_error else requests.HTTPError("Http Error"),
        'json.return_value': json
    })
    type(response_mock).status_code = status_code
    type(response_mock).headers = {'X-RateLimit-Remaining': 1}
    type(response_mock).links = []
    return response_mock


@mock.patch("requests.Session.request")
class TestListReposByOrgMatchers(unittest.TestCase):

    def test_should_return_repositories_matching_wildcards(self, mocked_request):
        json_response = [
            {'id': 1, 'name': 'dag-repo1', 'full_name': 'org/dag-repo1', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 2, 'name': 'dag-repo2-tests', 'full_name': 'org/dag-repo2-tests', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 3, 'name': 'dag-repo3-docs', 'full_name': 'org/dag-repo3-docs', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 4, 'name': 'repo4-tests-docs', 'full_name': 'org/repo4-test-docs', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 5, 'name': 'repo-tests', 'full_name': 'org/repo-tests', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 6, 'name': 'repo6', 'full_name': 'org/repo', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
        ]

        mocked_request.return_value = get_response(200, json_response)

        repositories = tap_github.get_all_repositories(
            "org",
            excludes=["*tests*"],
            includes=["dag*", "*docs*"]
        )

        self.assertEqual(mocked_request.call_count, 1)
        self.assertEqual(2, len(repositories))
        self.assertEqual('dag-repo1', repositories[0]['name'])
        self.assertEqual('dag-repo3-docs', repositories[1]['name'])

    def test_should_return_all_repositories_matching_wildcards(self, mocked_request):
        json_response = [
            {'id': 1, 'name': 'dag-repo1', 'full_name': 'org/dag-repo1', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 2, 'name': 'dag-repo2-tests', 'full_name': 'org/dag-repo2-tests', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 3, 'name': 'dag-repo3-docs', 'full_name': 'org/dag-repo3-docs', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 4, 'name': 'repo4-tests-docs', 'full_name': 'org/repo4-test-docs', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 5, 'name': 'repo-tests', 'full_name': 'org/repo-tests', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 6, 'name': 'repo6', 'full_name': 'org/repo', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
        ]

        mocked_request.return_value = get_response(200, json_response)

        repositories = tap_github.get_all_repositories(
            "org",
            includes=["*"]
        )

        self.assertEqual(mocked_request.call_count, 1)
        self.assertEqual(6, len(repositories))
        self.assertEqual('dag-repo1', repositories[0]['name'])
        self.assertEqual('dag-repo2-tests', repositories[1]['name'])
        self.assertEqual('dag-repo3-docs', repositories[2]['name'])
        self.assertEqual('repo4-tests-docs', repositories[3]['name'])
        self.assertEqual('repo-tests', repositories[4]['name'])
        self.assertEqual('repo6', repositories[5]['name'])



