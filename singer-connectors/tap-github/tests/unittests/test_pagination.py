import json
import math
import unittest
from unittest import mock

from requests import Response
from tap_github import get_review_comments_for_pr

response_record = {
    "type": "RECORD",
    "stream": "review_comments",
    "time_extracted": "2000-01-01T00:00:00.000000Z",
}


def generate_paginated_response(total_records, records_per_page):
    for page in range(math.ceil(total_records / records_per_page)):
        items_in_page = min(records_per_page, total_records - (records_per_page * page))
        response_content = [response_record for _ in range(items_in_page)]
        resp = Response()
        resp._content = json.dumps(response_content).encode()
        yield resp


class TestReviewComments(unittest.TestCase):
    def test_review_comments_pagination(self):
        records = [0, 15, 30, 45]
        records_per_page = 30
        for total_records in records:
            with self.subTest(
                f"testing review comments pagination with {total_records} records"
            ):
                with mock.patch("tap_github.authed_get_all_pages") as mocked_allpage:
                    mocked_allpage.return_value = generate_paginated_response(
                        total_records, records_per_page
                    )
                    review_comments_generator = get_review_comments_for_pr(
                        pr_number=452,
                        schema={},
                        repo_path="transferwise/pipelinewise",
                        state=None,
                        mdata={},
                    )
                    counter = 0
                    for _ in review_comments_generator:
                        counter += 1
                self.assertEqual(counter, total_records)
