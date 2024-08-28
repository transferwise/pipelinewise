import unittest
from unittest import mock
import tap_github.__init__ as tap_github

class Mockresponse:
    def __init__(self, resp):
        self.json_data = resp
        self.content = "github"

    def json(self):
        return [(self.json_data)]

def get_response(json):
    yield Mockresponse(resp=json)

@mock.patch("tap_github.__init__.authed_get_all_pages")
class TestKeyErrorSlug(unittest.TestCase):

    @mock.patch("tap_github.__init__.get_all_team_members")
    def test_slug_sub_stream_selected_slug_selected(self, mocked_team_members, mocked_request):
        json = {"key": "value", "slug": "team-slug"}

        mocked_request.return_value = get_response(json)

        schemas = {"teams": "None", "team_members": "None"}
        mdata  =[
        {
            'breadcrumb': [], 
            'metadata': {'selected': True, 'table-key-properties': ['id']}
        }, 
        {
            'breadcrumb': ['properties', 'slug'], 
            'metadata': {'inclusion': 'available'}
        }, 
        {
            "breadcrumb": [ "properties", "name"],
            "metadata": {"inclusion": "available"}
        }]
        tap_github.get_all_teams(schemas, "tap-github", {}, mdata, "")
        self.assertEqual(mocked_team_members.call_count, 1)

    @mock.patch("tap_github.__init__.get_all_team_members")
    def test_slug_sub_stream_not_selected_slug_selected(self, mocked_team_members, mocked_request):
        json = {"key": "value", "slug": "team-slug"}

        mocked_request.return_value = get_response(json)

        schemas = {"teams": "None"}
        mdata  =[
        {
            'breadcrumb': [], 
            'metadata': {'selected': True, 'table-key-properties': ['id']}
        }, 
        {
            'breadcrumb': ['properties', 'slug'], 
            'metadata': {'inclusion': 'available'}
        }, 
        {
            "breadcrumb": [ "properties", "name"],
            "metadata": {"inclusion": "available"}
        }]
        tap_github.get_all_teams(schemas, "tap-github", {}, mdata, "")
        self.assertEqual(mocked_team_members.call_count, 0)

    @mock.patch("tap_github.__init__.get_all_team_members")
    def test_slug_sub_stream_selected_slug_not_selected(self, mocked_team_members, mocked_request):
        json = {"key": "value", "slug": "team-slug"}

        mocked_request.return_value = get_response(json)

        schemas = {"teams": "None", "team_members": "None"}
        mdata  =[
        {
            'breadcrumb': [], 
            'metadata': {'selected': True, 'table-key-properties': ['id']}
        }, 
        {
            'breadcrumb': ['properties', 'slug'], 
            'metadata': {'inclusion': 'available', 'selected': False}
        }, 
        {
            "breadcrumb": [ "properties", "name"],
            "metadata": {"inclusion": "available"}
        }]
        tap_github.get_all_teams(schemas, "tap-github", {}, mdata, "")
        self.assertEqual(mocked_team_members.call_count, 1)

    @mock.patch("tap_github.__init__.get_all_team_members")
    def test_slug_sub_stream_not_selected_slug_not_selected(self, mocked_team_members, mocked_request):
        json = {"key": "value", "slug": "team-slug"}

        mocked_request.return_value = get_response(json)

        schemas = {"teams": "None"}
        mdata  =[
        {
            'breadcrumb': [], 
            'metadata': {'selected': True, 'table-key-properties': ['id']}
        }, 
        {
            'breadcrumb': ['properties', 'slug'], 
            'metadata': {'inclusion': 'available', 'selected': False}
        }, 
        {
            "breadcrumb": [ "properties", "name"],
            "metadata": {"inclusion": "available"}
        }]
        tap_github.get_all_teams(schemas, "tap-github", {}, mdata, "")
        self.assertEqual(mocked_team_members.call_count, 0)

@mock.patch("tap_github.__init__.authed_get_all_pages")
class TestKeyErrorUser(unittest.TestCase):

    @mock.patch("singer.write_record")
    def test_user_not_selected_in_stargazers(self, mocked_write_records, mocked_request):
        json = {"key": "value", "user": {"id": 1}}

        mocked_request.return_value = get_response(json)

        schemas = {"teams": "None"}
        mdata  =[
        {
            'breadcrumb': [], 
            'metadata': {'selected': True, 'table-key-properties': ['user_id']}
        },
        {
          "breadcrumb": ["properties", "user"],
          "metadata": {"inclusion": "available", "selected": False}
        },
        {
          "breadcrumb": ["properties", "starred_at"],
          "metadata": {"inclusion": "available"}
        }]
        tap_github.get_all_stargazers(schemas, "tap-github", {}, mdata, "")
        self.assertEqual(mocked_write_records.call_count, 1)

    @mock.patch("singer.write_record")
    def test_user_selected_in_stargazers(self, mocked_write_records, mocked_request):
        json = {"key": "value", "user": {"id": 1}}

        mocked_request.return_value = get_response(json)

        schemas = {"stargazers": "None"}
        mdata  =[
        {
            'breadcrumb': [], 
            'metadata': {'selected': True, 'table-key-properties': ['user_id']}
        },
        {
          "breadcrumb": ["properties", "user"],
          "metadata": {"inclusion": "available"}
        },
        {
          "breadcrumb": ["properties", "starred_at"],
          "metadata": {"inclusion": "available"}
        }]
        tap_github.get_all_stargazers(schemas, "tap-github", {}, mdata, "")
        self.assertEqual(mocked_write_records.call_count, 1)
