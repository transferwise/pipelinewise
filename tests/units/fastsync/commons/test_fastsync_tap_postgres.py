import pytest
from unittest import TestCase
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres


class TestFastSyncTapPostgres(TestCase):
    """
    Unit tests for fastsync tap postgres
    """
    def setUp(self) -> None:
        self.maxDiff = None

    def test_generate_replication_slot_name(self):
        """Validate if the replication slot name generated correctly"""
        postgres = FastSyncTapPostgres(connection_config={}, tap_type_to_target_type={})

        # Provide only database name
        assert postgres.generate_replication_slot_name('some_db') == 'pipelinewise_some_db'

        # Provide database name and tap_id
        assert postgres.generate_replication_slot_name('some_db',
                                                       'some_tap') == 'pipelinewise_some_db_some_tap'

        # Provide database name, tap_id and prefix
        assert postgres.generate_replication_slot_name('some_db',
                                                       'some_tap',
                                                       prefix='custom_prefix') == 'custom_prefix_some_db_some_tap'

        # Replication slot name should be lowercase
        assert postgres.generate_replication_slot_name('SoMe_DB',
                                                       'SoMe_TaP') == 'pipelinewise_some_db_some_tap'
