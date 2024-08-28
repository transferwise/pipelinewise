import contextlib
import io
import unittest
from unittest.mock import patch

from tap_s3_csv import do_discover


class InitTestCase(unittest.TestCase):

    @patch('tap_s3_csv.discover_streams')
    def test_do_discover(self, discover_streams):
        discover_streams.return_value = [{'stream': '1'}, {'stream': '2'}]

        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            do_discover({})

        self.assertEqual(
"""{
  "streams": [
    {
      "stream": "1"
    },
    {
      "stream": "2"
    }
  ]
}""", f.getvalue())
