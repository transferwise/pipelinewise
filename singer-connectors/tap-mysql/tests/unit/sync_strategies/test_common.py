import datetime

from singer.catalog import CatalogEntry
from singer.schema import Schema

from tap_mysql.sync_strategies import common


class TestCommonSyncStrategyHelpers:

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
