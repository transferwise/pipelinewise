import contextlib
import io
import os.path
import unittest
import boto3
import ujson
import random

from copy import deepcopy

from tap_s3_csv import do_discover, do_sync
from .utils import get_test_config


class TestTapS3Csv(unittest.TestCase):
    """
    Integration Tests
    """
    s3_client = None
    obj_name = None
    config = None
    maxDiff = None
    expected_catalog = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.config = get_test_config()
        random_prefix = f'tap_s3_csv_test_data_{random.randint(1, 10000)}'

        cls.config['tables'] = [{
            "search_prefix": random_prefix,
            "search_pattern": "users.csv",
            "table_name": "users",
            "key_properties": ["id"],
            "delimiter": ",",
            "date_overrides": ["birth_date"]
        }]

        file_name = os.path.join(os.path.dirname(__file__), 'mock_data.csv')
        cls.obj_name = f'{random_prefix}/users.csv'

        boto3.setup_default_session(
            aws_access_key_id=cls.config['aws_access_key_id'],
            aws_secret_access_key=cls.config['aws_secret_access_key'],
        )

        # upload test file to bucket
        s3_client = boto3.client('s3', endpoint_url=cls.config['aws_endpoint_url'])
        s3_client.upload_file(file_name, cls.config['bucket'], cls.obj_name)

        cls.expected_catalog = {
            "streams": [
                {
                    "stream": "users",
                    "tap_stream_id": "users",
                    "schema": {
                        "properties": {
                            "_sdc_extra": {
                                "items": {
                                    "type": "string"
                                },
                                "type": "array",
                            },
                            "_sdc_source_bucket": {
                                "type": "string"
                            },
                            "_sdc_source_file": {
                                "type": "string"
                            },
                            "_sdc_source_lineno": {
                                "type": "integer"
                            },
                            "birth_date": {
                                "type": ['null', "string"],
                                "format": "date-time"
                            },
                            "email": {
                                "type": ['null', "string"]
                            },
                            "first_name": {
                                "type": ['null', "string"]
                            },
                            "id": {
                                "type": ['null', "string"]
                            },
                            "ip_address": {
                                "type": ['null', "string"]
                            },
                            "is_pensioneer": {
                                "type": ["null", "string"]
                            },
                            "gender": {
                                "type": ['null', "string"]
                            },
                            "group": {
                                "type": ['null', "string"]
                            },
                            "last_name": {
                                "type": ['null', "string"]
                            },
                        },
                        "type": "object",
                    },
                    "metadata": [
                        {"breadcrumb": [], "metadata": {'table-key-properties': ['id']}},
                        {"breadcrumb": ["properties", "_sdc_extra"], "metadata": {"inclusion": "available"}},
                        {"breadcrumb": ["properties", "_sdc_source_bucket"], "metadata": {"inclusion": "available"}},
                        {"breadcrumb": ["properties", "_sdc_source_file"], "metadata": {"inclusion": "available"}},
                        {"breadcrumb": ["properties", "_sdc_source_lineno"], "metadata": {"inclusion": "available"}},
                        {"breadcrumb": ["properties", "birth_date"], "metadata": {"inclusion": "available"}},
                        {"breadcrumb": ["properties", "email"], "metadata": {"inclusion": "available"}},
                        {"breadcrumb": ["properties", "first_name"], "metadata": {"inclusion": "available"}},
                        {"breadcrumb": ["properties", "gender"], "metadata": {"inclusion": "available"}},
                        {"breadcrumb": ["properties", "group"], "metadata": {"inclusion": "available"}},
                        {"breadcrumb": ["properties", "id"], "metadata": {"inclusion": "automatic"}},
                        {"breadcrumb": ["properties", "ip_address"], "metadata": {"inclusion": "available"}},
                        {"breadcrumb": ["properties", "is_pensioneer"], "metadata": {"inclusion": "available"}},
                        {"breadcrumb": ["properties", "last_name"], "metadata": {"inclusion": "available"}},
                    ]
                }
            ]
        }

    @classmethod
    def tearDownClass(cls) -> None:
        s3_client = boto3.client('s3', endpoint_url=cls.config['aws_endpoint_url'])
        s3_client.delete_object(Bucket=cls.config['bucket'], Key=cls.obj_name)

    def test_discovery(self):
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            do_discover(self.config)

        catalog = ujson.loads(f.getvalue())

        self.assertIsInstance(catalog, dict)
        self.assertEqual(1, len(catalog['streams']))
        self.assertEqual(self.expected_catalog['streams'][0]['stream'], catalog['streams'][0]['stream'])
        self.assertEqual(self.expected_catalog['streams'][0]['tap_stream_id'], catalog['streams'][0]['tap_stream_id'])

        self.assertDictEqual(self.expected_catalog['streams'][0]['schema'], catalog['streams'][0]['schema'])
        self.assertListEqual(self.expected_catalog['streams'][0]['metadata'],
                             # sort metadata to have have a fix order of breadcrumbs to make assertion pass
                             sorted(catalog['streams'][0]['metadata'], key=lambda d: d['breadcrumb']))

    def test_sync(self):
        catalog = deepcopy(self.expected_catalog)
        # set stream to selected
        catalog['streams'][0]['metadata'][0]['metadata']['selected'] = True

        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            do_sync(self.config, catalog, {})

        lines = [ujson.loads(line) for line in f.getvalue().strip().splitlines()]

        self.assertDictEqual({'type': 'STATE', 'value': {}}, lines[0])
        self.assertEqual('SCHEMA', lines[1]['type'])
        self.assertEqual('users', lines[1]['stream'])
        self.assertEqual(catalog['streams'][0]['schema'].keys(), lines[1]['schema'].keys())

        self.assertEqual('RECORD', lines[2]['type'])
        self.assertEqual('users', lines[2]['stream'])
        self.assertDictEqual({
                '_sdc_source_bucket': self.config['bucket'],
                '_sdc_source_file': self.obj_name,
                '_sdc_source_lineno': 2,
                'birth_date': '1971-01-22T00:00:00.000000Z',
                'email': 'gmackney0@china.com.cn',
                'first_name': 'Ginger',
                'id': '1',
                'ip_address': '180.48.88.217',
                'is_pensioneer': 'true',
                'gender': 'Male',
                'group': '90',
                'last_name': 'Mackney',
        }, lines[2]['record'], lines[2]['record'])

        self.assertIsNotNone(lines[2]['time_extracted'])

        self.assertEqual(100, sum(1 for line in lines if line['type'] == 'RECORD'))
