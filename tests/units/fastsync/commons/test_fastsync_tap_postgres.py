from unittest import TestCase
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres


class FastSyncTapPostgresMock(FastSyncTapPostgres):
    """
    Mocked FastSyncTapPostgres class
    """
    def __init__(self, connection_config, transformation_config=None):
        super().__init__(connection_config, transformation_config)

        self.executed_queries_primary_host = []
        self.executed_queries = []

    def primary_host_query(self, query, params=None):
        self.executed_queries_primary_host.append(query)
        return []

    def query(self, query, params=None):
        self.executed_queries.append(query)
        return []


# pylint: disable=invalid-name,no-self-use
class TestFastSyncTapPostgres(TestCase):
    """
    Unit tests for fastsync tap postgres
    """
    def setUp(self) -> None:
        """Initialise test FastSyncTapPostgres object"""
        self.postgres = FastSyncTapPostgresMock(connection_config={'dbname': 'test_database',
                                                                   'tap_id': 'test_tap'},
                                                transformation_config={})

    def test_generate_replication_slot_name(self):
        """Validate if the replication slot name generated correctly"""
        # Provide only database name
        assert self.postgres.generate_replication_slot_name('some_db') == 'pipelinewise_some_db'

        # Provide database name and tap_id
        assert self.postgres.generate_replication_slot_name('some_db',
                                                            'some_tap') == 'pipelinewise_some_db_some_tap'

        # Provide database name, tap_id and prefix
        assert self.postgres.generate_replication_slot_name('some_db',
                                                            'some_tap',
                                                            prefix='custom_prefix') == 'custom_prefix_some_db_some_tap'

        # Replication slot name should be lowercase
        assert self.postgres.generate_replication_slot_name('SoMe_DB',
                                                            'SoMe_TaP') == 'pipelinewise_some_db_some_tap'

        # Invalid characters should be replaced by underscores
        assert self.postgres.generate_replication_slot_name('some-db',
                                                            'some-tap') == 'pipelinewise_some_db_some_tap'

        assert self.postgres.generate_replication_slot_name('some.db',
                                                            'some.tap') == 'pipelinewise_some_db_some_tap'

    def test_create_replication_slot(self):
        """Validate if replication slot creation SQL commands generated correctly"""
        self.postgres.create_replication_slot()
        assert self.postgres.executed_queries_primary_host == [
            "SELECT * FROM pg_replication_slots WHERE slot_name = 'pipelinewise_test_database'",
            "SELECT * FROM pg_create_logical_replication_slot('pipelinewise_test_database_test_tap', 'wal2json')"
        ]
