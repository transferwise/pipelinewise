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
class TestListReposByOrg(unittest.TestCase):

    def test_should_return_a_list_of_all_repositories(self, mocked_request):
        json_response = [
            {'id': 1, 'name': 'repo1', 'full_name': 'org/repo1', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 2, 'name': 'repo2', 'full_name': 'org/repo2', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 3, 'name': 'repo3', 'full_name': 'org/repo3', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'}
        ]

        mocked_request.return_value = get_response(200, json_response)

        repositories = tap_github.get_all_repositories("org", [])

        self.assertEqual(mocked_request.call_count, 1)
        self.assertEqual(len(repositories), 3)
        self.assertEqual('repo1', repositories[0]['name'])
        self.assertEqual('repo2', repositories[1]['name'])
        self.assertEqual('repo3', repositories[2]['name'])

    def test_should_exclude_repositories_passed_in_the_exclude_parameters(self, mocked_request):
        json_response = [
            {'id': 1, 'name': 'repo1', 'full_name': 'org/repo1', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 2, 'name': 'repo2', 'full_name': 'org/repo2', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 3, 'name': 'repo3', 'full_name': 'org/repo3', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'}
        ]

        mocked_request.return_value = get_response(200, json_response)

        repositories = tap_github.get_all_repositories("org", ["repo3"])

        self.assertEqual(mocked_request.call_count, 1)
        self.assertEqual(len(repositories), 2)
        self.assertEqual('repo1', repositories[0]['name'])
        self.assertEqual('repo2', repositories[1]['name'])

    def test_should_exclude_repositories_marked_as_disabled_by_default(self, mocked_request):
        json_response = [
            {
                'id': 1,
                'name': 'repo1',
                'full_name': 'org/repo1',
                'size': 100,
                'updated_at': '2020-06-12T13:12:55Z',
                'disabled': True
            },
            {
                'id': 2,
                'name': 'repo2',
                'full_name': 'org/repo2',
                'size': 100,
                'updated_at': '2020-06-12T13:12:55Z',
                'disabled': False
            }
        ]

        mocked_request.return_value = get_response(200, json_response)

        repositories = tap_github.get_all_repositories("org")

        self.assertEqual(mocked_request.call_count, 1)
        self.assertEqual(len(repositories), 1)
        self.assertEqual('repo2', repositories[0]['name'])

    def test_should_include_repositories_marked_as_disabled(self, mocked_request):
        json_response = [
            {
                'id': 1,
                'name': 'repo1',
                'full_name': 'org/repo1',
                'size': 100,
                'updated_at': '2020-06-12T13:12:55Z',
                'disabled': True
            },
            {
                'id': 2,
                'name': 'repo2',
                'full_name': 'org/repo2',
                'size': 100,
                'updated_at': '2020-06-12T13:12:55Z',
                'disabled': True
            }
        ]

        mocked_request.return_value = get_response(200, json_response)

        repositories = tap_github.get_all_repositories("org", include_disabled=True)

        self.assertEqual(mocked_request.call_count, 1)
        self.assertEqual(len(repositories), 2)
        self.assertEqual('repo1', repositories[0]['name'])
        self.assertEqual('repo2', repositories[1]['name'])

    def test_should_exclude_repositories_marked_as_archived_by_default(self, mocked_request):
        json_response = [
            dict(
                id=1,
                name='repo1',
                full_name='org/repo1',
                size=100,
                updated_at='2020-06-12T13:12:55Z',
                archived=False
            ),
            dict(
                id=2,
                name='repo2',
                full_name='org/repo2',
                size=100,
                updated_at='2020-06-12T13:12:55Z',
                archived=True
            )
        ]

        mocked_request.return_value = get_response(200, json_response)

        repositories = tap_github.get_all_repositories("org", [])

        self.assertEqual(mocked_request.call_count, 1)
        self.assertEqual(len(repositories), 1)
        self.assertEqual('repo1', repositories[0]['name'])

    def test_should_include_repositories_marked_as_archived(self, mocked_request):
        json_response = [
            dict(
                id=1,
                name='repo1',
                full_name='org/repo1',
                size=100,
                updated_at='2020-06-12T13:12:55Z',
                archived=True
            ),
            dict(
                id=2,
                name='repo2',
                full_name='org/repo2',
                size=100,
                updated_at='2020-06-12T13:12:55Z',
                archived=True
            )
        ]

        mocked_request.return_value = get_response(200, json_response)

        repositories = tap_github.get_all_repositories("org", include_archived=True)

        self.assertEqual(mocked_request.call_count, 1)
        self.assertEqual(len(repositories), 2)
        self.assertEqual('repo1', repositories[0]['name'])
        self.assertEqual('repo2', repositories[1]['name'])

    def test_should_ignore_empty_repos(self, mocked_request):
        json_response = [
            {'id': 1, 'name': 'repo1', 'full_name': 'org/repo1', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 2, 'name': 'repo2', 'full_name': 'org/repo2', 'size': 0, 'updated_at': '2020-06-12T13:12:55Z'},
            {'id': 3, 'name': 'repo3', 'full_name': 'org/repo3', 'size': 100, 'updated_at': '2020-06-12T13:12:55Z'}
        ]

        mocked_request.return_value = get_response(200, json_response)

        repositories = tap_github.get_all_repositories("org", [])

        self.assertEqual(mocked_request.call_count, 1)
        self.assertEqual(len(repositories), 2)
        self.assertEqual('repo1', repositories[0]['name'])
        self.assertEqual('repo3', repositories[1]['name'])