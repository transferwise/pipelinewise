import unittest
from unittest import mock
import tap_github.__init__ as tap_github

@mock.patch("tap_github.__init__.authed_get_all_pages")
class TestStargazersFullTable(unittest.TestCase):

    def test_stargazers_without_query_params(self, mocked_request):

        schemas = {"stargazers": "None"}

        tap_github.get_all_stargazers(schemas, "tap-github", {}, {}, "")

        mocked_request.assert_called_with(mock.ANY, "https://api.github.com/repos/tap-github/stargazers", mock.ANY)
