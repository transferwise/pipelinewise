import unittest

from tap_s3_csv.s3 import generate_schema


class TestS3(unittest.TestCase):
    def test_generate_schema(self):
        samples = [
            dict(id='1', name='productA', added_at='2017/05/18 10:40:22', price='22.99', sold='true',
                 sold_at='2019-11-29'),
            dict(id='4', name='productB', added_at='2017/05/18 10:40:22', price='18', sold='false'),
            dict(id='6', name='productC', added_at='2017/05/18 10:40:22', price='14.6', sold='true',
                 sold_at='2019-12-11'),
        ]

        table_specs = {
            'date_overrides': ['added_at']
        }

        schema = generate_schema(samples, table_specs)

        self.assertDictEqual({
            'id': {
                'type': ['null', 'string']
            },
            'name': {
                'type': ['null', 'string']
            },
            'added_at': {'type': ['null', 'string'], 'format': 'date-time'},
            'price': {
                'type': ['null', 'string']
            },
            'sold': {
                'type': ['null', 'string']
            },
            'sold_at': {'type': ['null', 'string']}
        }, schema)
