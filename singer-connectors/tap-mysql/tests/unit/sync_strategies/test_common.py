import datetime
import json
from pathlib import Path

from singer.catalog import CatalogEntry
from singer.schema import Schema

from tap_mysql.sync_strategies import common


REPOSITORY_CONTRACT_PATH = (
    Path(__file__).resolve().parents[5]
    / 'tests'
    / 'resources'
    / 'snowflake_managed_iceberg_contract.json'
)


def _mariadb_json_transport_case():
    with REPOSITORY_CONTRACT_PATH.open(encoding='utf-8') as contract_file:
        contract = json.load(contract_file)
    return contract['behavior_cases']['3']['mariadb_json_transport']


class TestCommonSyncStrategyHelpers:

    def test_is_invalid_mysql_datetime(self):
        assert common.is_invalid_mysql_datetime('INVALID_MYSQL_DATETIME')
        assert common.is_invalid_mysql_datetime('0000-00-00 00:00:00')
        assert common.is_invalid_mysql_datetime('2024-02-29T12:00:00+99:00')
        assert common.is_invalid_mysql_datetime(b'0000-00-00 00:00:00')

        assert not common.is_invalid_mysql_datetime('2024-02-29T12:00:00+00:00')
        assert not common.is_invalid_mysql_datetime(None)
        assert not common.is_invalid_mysql_datetime(4)
        assert not common.is_invalid_mysql_datetime([datetime.datetime.now(), datetime.datetime.now()])
        assert not common.is_invalid_mysql_datetime(['Foo', 'bar'])

    def test_row_to_singer_record(self):
        catalog_entry = CatalogEntry(
            stream='stream',
            schema=Schema.from_dict({
                'type': 'object',
                'properties': {
                    'time': {
                        'type': 'string',
                        'format': 'time',
                    },
                },
            }),
        )
        message = common.row_to_singer_record(
            catalog_entry,
            version=1,
            row=(datetime.timedelta(hours=8, minutes=30),),
            columns=['time'],
            time_extracted=datetime.datetime.now(datetime.timezone.utc),
        )

        assert message.stream == 'stream'
        assert message.version == 1
        assert message.record == {'time': '08:30:00'}
        assert message.time_extracted is not None

    def test_row_to_singer_record_preserves_every_mariadb_json_root_as_text(self):
        case = _mariadb_json_transport_case()
        encoded_values = {
            f'root_{index}': value
            for index, value in enumerate(case['serialized_roots'])
        }
        encoded_values['root_0'] = encoded_values['root_0'].encode('utf-8')
        encoded_values['sql_null'] = None
        catalog_entry = CatalogEntry(
            stream='stream',
            schema=Schema.from_dict({
                'type': 'object',
                'properties': {
                    column: case['schema']
                    for column in encoded_values
                },
            }),
        )

        message = common.row_to_singer_record(
            catalog_entry,
            version=1,
            row=tuple(encoded_values.values()),
            columns=list(encoded_values),
            time_extracted=datetime.datetime.now(datetime.timezone.utc),
        )

        assert list(message.record.values())[:-1] == case['serialized_roots']
        assert message.record['sql_null'] is None

    def test_row_to_singer_record_only_nulls_invalid_datetime_values(self):
        datetime_values = {
            'valid_datetime': '2024-02-29T12:00:00+00:00',
            'zero_datetime': '0000-00-00 00:00:00',
            'zero_year': '0000-01-01 00:00:00',
            'invalid_month': '2024-13-01 12:00:00',
            'zero_day': '2024-05-00 12:00:00',
            'invalid_day': '2024-02-30 12:00:00',
            'invalid_timezone': '2024-02-29T12:00:00+99:00',
            'invalid_bytes': b'0000-00-00 00:00:00',
        }
        values = {**datetime_values, 'ordinary_string': 'not-a-date'}
        catalog_entry = CatalogEntry(
            stream='stream',
            schema=Schema.from_dict({
                'type': 'object',
                'properties': {
                    **{
                        column: {
                            'type': ['null', 'string'],
                            'format': 'date-time',
                        }
                        for column in datetime_values
                    },
                    'ordinary_string': {'type': ['null', 'string']},
                },
            }),
        )

        message = common.row_to_singer_record(
            catalog_entry,
            version=1,
            row=tuple(values.values()),
            columns=list(values),
            time_extracted=datetime.datetime.now(datetime.timezone.utc),
        )

        assert message.record == {
            'valid_datetime': '2024-02-29T12:00:00+00:00',
            'zero_datetime': None,
            'zero_year': None,
            'invalid_month': None,
            'zero_day': None,
            'invalid_day': None,
            'invalid_timezone': None,
            'invalid_bytes': None,
            'ordinary_string': 'not-a-date',
        }
