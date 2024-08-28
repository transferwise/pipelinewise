from unittest import mock
import tap_github
import unittest
import requests

class Mockresponse:
    def __init__(self, status_code, json, raise_error, headers={'X-RateLimit-Remaining': 1}, text=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers
        self.content = "github"

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        return self.text

def get_response(status_code, json={}, raise_error=False):
    return Mockresponse(status_code, json, raise_error)

@mock.patch("requests.Session.request")
class TestCredentials(unittest.TestCase):

    def test_repo_not_found(self, mocked_request):
        json = {"message": "Not Found", "documentation_url": "https:/docs.github.com/"}
        mocked_request.return_value = get_response(404, json, True)

        try:
            tap_github.verify_repo_access("", "repo")
        except tap_github.NotFoundException as e:
            self.assertEqual(str(e), "HTTP-error-code: 404, Error: Please check the repository name 'repo' or you do not have sufficient permissions to access this repository.")

    def test_repo_bad_request(self, mocked_request):
        mocked_request.return_value = get_response(400, raise_error = True)

        try:
            tap_github.verify_repo_access("", "repo")
        except tap_github.BadRequestException as e:
            self.assertEqual(str(e), "HTTP-error-code: 400, Error: The request is missing or has a bad parameter.")

    def test_repo_bad_creds(self, mocked_request):
        json = {"message": "Bad credentials", "documentation_url": "https://docs.github.com/"}
        mocked_request.return_value = get_response(401, json, True)

        try:
            tap_github.verify_repo_access("", "repo")
        except tap_github.BadCredentialsException as e:
            self.assertEqual(str(e), "HTTP-error-code: 401, Error: {}".format(json))

    @mock.patch("tap_github.get_catalog")
    def test_discover_valid_creds(self, mocked_get_catalog, mocked_request):
        mocked_request.return_value = get_response(200)
        mocked_get_catalog.return_value = {}

        tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})

        self.assertTrue(mocked_get_catalog.call_count, 1)

    @mock.patch("tap_github.get_catalog")
    def test_discover_not_found(self, mocked_get_catalog, mocked_request):
        json = {"message": "Not Found", "documentation_url": "https:/docs.github.com/"}
        mocked_request.return_value = get_response(404, json, True)
        mocked_get_catalog.return_value = {}

        try:
            tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})
        except tap_github.NotFoundException as e:
                self.assertEqual(str(e), "HTTP-error-code: 404, Error: Please check the repository name 'org/repo' or you do not have sufficient permissions to access this repository.")
        self.assertEqual(mocked_get_catalog.call_count, 0)

    @mock.patch("tap_github.get_catalog")
    def test_discover_bad_request(self, mocked_get_catalog, mocked_request):
        mocked_request.return_value = get_response(400, raise_error = True)
        mocked_get_catalog.return_value = {}

        try:
            tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})
        except tap_github.BadRequestException as e:
                self.assertEqual(str(e), "HTTP-error-code: 400, Error: The request is missing or has a bad parameter.")
        self.assertEqual(mocked_get_catalog.call_count, 0)

    @mock.patch("tap_github.get_catalog")
    def test_discover_bad_creds(self, mocked_get_catalog, mocked_request):
        json = {"message":"Bad credentials","documentation_url":"https://docs.github.com/"}
        mocked_request.return_value = get_response(401, json, True)
        mocked_get_catalog.return_value = {}

        try:
            tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})
        except tap_github.BadCredentialsException as e:
                self.assertEqual(str(e), "HTTP-error-code: 401, Error: {}".format(json))
        self.assertEqual(mocked_get_catalog.call_count, 0)

    @mock.patch("tap_github.get_catalog")
    def test_discover_forbidden(self, mocked_get_catalog, mocked_request):
        json = {'message': 'Must have admin rights to Repository.', 'documentation_url': 'https://docs.github.com/'}
        mocked_request.return_value = get_response(403, json, True)
        mocked_get_catalog.return_value = {}

        try:
            tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})
        except tap_github.AuthException as e:
                self.assertEqual(str(e), "HTTP-error-code: 403, Error: {}".format(json))
        self.assertEqual(mocked_get_catalog.call_count, 0)


@mock.patch("tap_github.logger.info")
@mock.patch("tap_github.verify_repo_access")
class TestRepoCallCount(unittest.TestCase):
    def test_repo_call_count(self, mocked_repo, mocked_logger_info):
        """
            Here 3 repos are given,
            so tap will check creds for all 3 repos
        """
        mocked_repo.return_value = None

        config = {"access_token": "access_token", "repository": "org1/repo1 org1/repo2 org2/repo1"}
        tap_github.verify_access_for_repo(config)

        self.assertEqual(mocked_logger_info.call_count, 3)
        self.assertEqual(mocked_repo.call_count, 3)
