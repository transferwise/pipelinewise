import datetime
import os
import sys
import unittest

import json
from io import StringIO

import pytz
from singer import Catalog

from tap_zendesk import do_sync
from tests.helper.zenpymock import ZenpyMock

DIR = os.path.dirname(__file__)

class DoSync(unittest.TestCase):
    def setUp(self):
        self.catalog = Catalog.load(os.path.join(DIR, 'test_catalog.json'))

        with open(os.path.join(DIR, 'test_state.json')) as f:
            self.state = json.load(f)

        self.start_date = "2019-11-12T06:47:14.000000Z"

    def test_network_failure(self):
        client = ZenpyMock(n_tickets=10000, p_sleep=0.01, p_failure=0.1, subdomain='xyz', oauth_token=123)

        with self.assertRaises(RuntimeError):
            do_sync(client, self.catalog, self.state, self.start_date)

    def test_data_consistency(self):
        client = ZenpyMock(n_tickets=1000, p_sleep=0.01, subdomain='xyz', oauth_token=123)

        saved_stdout = sys.stdout

        string_io = StringIO()
        sys.stdout = string_io

        # Run do_sync
        do_sync(client, self.catalog, self.state, self.start_date)
        sys.stdout = saved_stdout

        # Checks that every message got delivered on time
        self._check_everything_delivered(string_io.getvalue(), client)

        # Checks that STATE messages are not corrupted
        self._check_state_consistency(string_io.getvalue())

    def _parse_stdout(self, stdout):
        stdout_messages = []

        # Process only json messages
        for s in stdout.split("\n"):
            try:
                stdout_messages.append(json.loads(s))
            except Exception as e:
                pass

        return stdout_messages

    def _check_state_consistency(self, stdout):
        stdout_messages = self._parse_stdout(stdout)

        stream_to_last_record_ts = {}
        stream_to_last_state_ts = {}

        for msg in stdout_messages:
            if msg['type'] == 'RECORD':
                msg_stream = msg['stream']
                if msg_stream == 'tickets':
                    ts = datetime.datetime.utcfromtimestamp(msg['record']['generated_timestamp'])\
                        .replace(tzinfo=pytz.UTC)

                    # When we see the state message, all other messages have in the stream need to have at least
                    # this timestamp, otherwise we lose the data.
                    if msg_stream in stream_to_last_state_ts:
                        assert ts >= stream_to_last_state_ts[msg_stream]

                    if msg_stream in stream_to_last_record_ts:
                        stream_to_last_record_ts[msg_stream] = max(stream_to_last_record_ts[msg_stream], ts)
                    else:
                        stream_to_last_record_ts[msg_stream] = ts

            elif msg['type'] == 'STATE':
                bookmarks = msg['value']['bookmarks']

                for stream, v in bookmarks.items():
                    cur_ts = None
                    if 'generated_timestamp' in v:
                        cur_ts = self._convert_str_to_datetime(v['generated_timestamp'], use_microseconds=True)
                    elif 'updated_at' in v:
                        cur_ts = self._convert_str_to_datetime(v['updated_at'])

                    if stream in stream_to_last_state_ts:
                        stream_to_last_state_ts[stream] = max(stream_to_last_state_ts[stream], cur_ts)
                    else:
                        stream_to_last_state_ts[stream] = cur_ts

    def _convert_str_to_datetime(self, s, use_microseconds=False):
        return datetime.datetime.strptime(s, f"%Y-%m-%dT%H:%M:%S{'.%f' if use_microseconds else ''}Z")\
            .replace(tzinfo=pytz.UTC)

    def _check_everything_delivered(self, stdout, client):
        stdout_messages = self._parse_stdout(stdout)

        generated_tickets = list(map(lambda x: x.to_dict(), client.generated_tickets))
        for ticket in generated_tickets:
            ticket.pop('fields')

        generated_audits = client.generated_audits
        generated_metrics = client.generated_metrics
        generated_comments = {k: list(map(lambda x: x.to_dict(), v)) for k, v in client.generated_comments.items()}

        for msg in stdout_messages:
            if msg['type'] != 'RECORD':
                continue

            record = msg['record']

            if msg['stream'] == 'tickets':
                generated_tickets.remove(record)
            elif msg['stream'] == 'ticket_audits':
                generated_audits[record['ticket_id']].remove(record)
            elif msg['stream'] == 'ticket_metrics':
                for metric in record:
                    generated_metrics[metric['ticket_id']].remove(metric)
            elif msg['stream'] == 'ticket_comments':
                generated_comments[record['ticket_id']].remove(record)

        # All the tickets that we generated in client have been observed as a part of the output
        assert len(generated_tickets) == 0

        # All the audits, metrics and comments that we generated in client have been observed as a part of the output
        for ticket_id, audits in generated_audits.items():
            assert audits == [], f"{ticket_id}. {audits}"

        for ticket_id, metrics in generated_metrics.items():
            assert metrics == [], f"{ticket_id}. {metrics}"

        for ticket_id, comments in generated_comments.items():
            assert comments == [], f"{ticket_id}. {comments}"


if __name__ == '__main__':
    unittest.main()
