import unittest

from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry, write_catalog


class TestWriteCatalog(unittest.TestCase):
    def test_write_empty_catalog(self):
        catalog = Catalog([])
        write_catalog(catalog)

    def test_write_catalog_with_streams(self):
        catalog = Catalog([CatalogEntry(tap_stream_id='a',schema=Schema(),metadata=[])])
        write_catalog(catalog)


class TestGetSelectedStreams(unittest.TestCase):
    def test_one_selected_stream(self):
        selected_entry = CatalogEntry(tap_stream_id='a',
                                      schema=Schema(),
                                      metadata=[{'metadata':
                                                 {'selected': True},
                                                 'breadcrumb': []}])
        catalog = Catalog(
            [selected_entry,
             CatalogEntry(tap_stream_id='b',schema=Schema(),metadata=[]),
             CatalogEntry(tap_stream_id='c',schema=Schema(),metadata=[])])
        state = {}
        selected_streams = catalog.get_selected_streams(state)
        self.assertEqual([e for e in selected_streams],[selected_entry])

    def test_resumes_currently_syncing_stream(self):
        selected_entry_a = CatalogEntry(tap_stream_id='a',
                                      schema=Schema(),
                                      metadata=[{'metadata':
                                                 {'selected': True},
                                                 'breadcrumb': []}])
        selected_entry_c = CatalogEntry(tap_stream_id='c',
                                        schema=Schema(),
                                        metadata=[{'metadata':
                                                   {'selected': True},
                                                   'breadcrumb': []}])
        catalog = Catalog(
            [selected_entry_a,
             CatalogEntry(tap_stream_id='b',schema=Schema(),metadata=[]),
             selected_entry_c])
        state = {'currently_syncing': 'c'}
        selected_streams = catalog.get_selected_streams(state)
        self.assertEqual([e for e in selected_streams][0],selected_entry_c)


class TestToDictAndFromDict(unittest.TestCase):

    dict_form = {
        'streams': [
            {
                'stream': 'users',
                'tap_stream_id': 'prod_users',
                'stream_alias': 'users_alias',
                'database_name': 'prod',
                'table_name': 'users',
                'schema': {
                    'type': 'object',
                    'selected': True,
                    'properties': {
                        'id': {'type': 'integer', 'selected': True},
                        'name': {'type': 'string', 'selected': True}
                    }
                },
                'metadata': [
                    {
                        'metadata': {
                            'metadata-key': 'metadata-value'
                        },
                        'breadcrumb': [
                            'properties',
                            'name',
                        ],
                    },
                ],
            },
            {
                'stream': 'orders',
                'tap_stream_id': 'prod_orders',
                'database_name': 'prod',
                'table_name': 'orders',
                'schema': {
                    'type': 'object',
                    'selected': True,
                    'properties': {
                        'id': {'type': 'integer', 'selected': True},
                        'amount': {'type': 'number', 'selected': True}
                        }
                    }
                }
            ]
        }

    obj_form = Catalog(streams=[
        CatalogEntry(
            stream='users',
            tap_stream_id='prod_users',
            stream_alias='users_alias',
            database='prod',
            table='users',
            schema=Schema(
                type='object',
                selected=True,
                properties={
                    'id': Schema(type='integer', selected=True),
                    'name': Schema(type='string', selected=True)}),
            metadata=[{
                'metadata': {
                    'metadata-key': 'metadata-value'
                },
                'breadcrumb': [
                    'properties',
                    'name',
                ],
            }]),
        CatalogEntry(
            stream='orders',
            tap_stream_id='prod_orders',
            database='prod',
            table='orders',
            schema=Schema(
                type='object',
                selected=True,
                properties={
                    'id': Schema(type='integer', selected=True),
                    'amount': Schema(type='number', selected=True)}))])

    def test_from_dict(self):
        self.assertEqual(self.obj_form, Catalog.from_dict(self.dict_form))

    def test_to_dict(self):
        self.assertEqual(self.dict_form, self.obj_form.to_dict())


class TestGetStream(unittest.TestCase):
    def test(self):
        catalog = Catalog(
            [CatalogEntry(tap_stream_id='a'),
             CatalogEntry(tap_stream_id='b'),
             CatalogEntry(tap_stream_id='c')])
        entry = catalog.get_stream('b')
        self.assertEqual('b', entry.tap_stream_id)
