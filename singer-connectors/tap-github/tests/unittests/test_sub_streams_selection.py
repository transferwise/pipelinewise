import unittest
import tap_github.__init__ as tap_github

class TestSubStreamSelection(unittest.TestCase):

    def test_pull_request_sub_streams_selected(self):
        selected_streams = ["reviews", "pull_requests"]
        self.assertIsNone(tap_github.validate_dependencies(selected_streams))

    def test_pull_request_sub_streams_not_selected(self):
        selected_streams = ["reviews", "pr_commits"]
        try:
            tap_github.validate_dependencies(selected_streams)
        except tap_github.DependencyException as e:
            self.assertEqual(str(e), "Unable to extract 'reviews' data, to receive 'reviews' data, you also need to select 'pull_requests'. Unable to extract 'pr_commits' data, to receive 'pr_commits' data, you also need to select 'pull_requests'.")

    def test_teams_sub_streams_selected(self):
        selected_streams = ["teams", "team_members"]
        self.assertIsNone(tap_github.validate_dependencies(selected_streams))

    def test_teams_sub_streams_not_selected(self):
        selected_streams = ["team_members"]
        try:
            tap_github.validate_dependencies(selected_streams)
        except tap_github.DependencyException as e:
            self.assertEqual(str(e), "Unable to extract 'team_members' data, to receive 'team_members' data, you also need to select 'teams'.")

    def test_projects_sub_streams_selected(self):
        selected_streams = ["projects", "project_cards"]
        self.assertIsNone(tap_github.validate_dependencies(selected_streams))

    def test_projects_sub_streams_not_selected(self):
        selected_streams = ["project_columns"]
        try:
            tap_github.validate_dependencies(selected_streams)
        except tap_github.DependencyException as e:
            self.assertEqual(str(e), "Unable to extract 'project_columns' data, to receive 'project_columns' data, you also need to select 'projects'.")

    def test_mixed_streams_positive(self):
        selected_streams = ["pull_requests", "reviews", "collaborators", "team_members", "stargazers", "projects", "teams", "project_cards"]
        self.assertIsNone(tap_github.validate_dependencies(selected_streams))

    def test_mixed_streams_negative(self):
        selected_streams = ["project_columns", "issues", "teams", "team_memberships", "projects", "releases", "review_comments"]
        try:
            tap_github.validate_dependencies(selected_streams)
        except tap_github.DependencyException as e:
            self.assertEqual(str(e), "Unable to extract 'review_comments' data, to receive 'review_comments' data, you also need to select 'pull_requests'.")
