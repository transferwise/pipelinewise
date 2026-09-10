import unittest
from unittest.mock import patch

from tap_yugabyte.sync_strategies import common


class TestShouldSyncColumn(unittest.TestCase):
    maxDiff = None

    def test_automatic_column_is_always_synced(self):
        """Automatic (key) columns are synced regardless of their selected flag"""
        md_map = {('properties', 'id'): {'inclusion': 'automatic', 'selected': False}}
        self.assertTrue(common.should_sync_column(md_map, 'id'))

    def test_available_column_synced_when_selected(self):
        """Available columns are synced only when explicitly selected"""
        md_map = {('properties', 'name'): {'inclusion': 'available', 'selected': True}}
        self.assertTrue(common.should_sync_column(md_map, 'name'))

    def test_available_column_not_synced_when_unselected(self):
        """Available columns are skipped when not selected"""
        md_map = {('properties', 'name'): {'inclusion': 'available', 'selected': False}}
        self.assertFalse(common.should_sync_column(md_map, 'name'))

    def test_unknown_column_defaults_to_synced(self):
        """A column missing from the metadata map falls back to should_sync_field's default=True"""
        self.assertTrue(common.should_sync_column({}, 'missing'))


class TestSendSchemaMessage(unittest.TestCase):
    maxDiff = None

    @patch('tap_yugabyte.sync_strategies.common.write_schema_message')
    def test_send_schema_message_for_table(self, mocked_write_schema_message):
        """The emitted SCHEMA message carries key properties and the destination stream name"""
        stream = {
            'stream': 'country',
            'schema': {'properties': {'code': {'type': 'string'}}},
            'metadata': [{
                'metadata': {'schema-name': 'public', 'table-key-properties': ['code'], 'is-view': False},
                'breadcrumb': [],
            }],
        }
        common.send_schema_message(stream, ['code'])

        mocked_write_schema_message.assert_called_once()
        schema_message = mocked_write_schema_message.call_args[0][0]
        self.assertEqual('SCHEMA', schema_message['type'])
        self.assertEqual('public-country', schema_message['stream'])
        self.assertEqual(['code'], schema_message['key_properties'])
        self.assertEqual(['code'], schema_message['bookmark_properties'])

    @patch('tap_yugabyte.sync_strategies.common.write_schema_message')
    def test_send_schema_message_for_view(self, mocked_write_schema_message):
        """Views use view-key-properties instead of table-key-properties"""
        stream = {
            'stream': 'a_view',
            'schema': {'properties': {}},
            'metadata': [{
                'metadata': {'schema-name': 'public', 'view-key-properties': ['id'], 'is-view': True},
                'breadcrumb': [],
            }],
        }
        common.send_schema_message(stream, [])

        schema_message = mocked_write_schema_message.call_args[0][0]
        self.assertEqual(['id'], schema_message['key_properties'])
