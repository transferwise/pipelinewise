"""Source logs report the endpoint used by the driver without credentials."""

import logging
from unittest.mock import Mock, patch

import pytest
import singer.logger as singer_logger

from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.commons.tap_mongodb import FastSyncTapMongoDB


@pytest.fixture(autouse=True)
def source_log_capture(monkeypatch, caplog):
    monkeypatch.setattr(singer_logger, '_REPORTED_SOURCE_HOSTS', set())
    monkeypatch.setattr(logging.getLogger('pipelinewise'), 'propagate', True)
    caplog.set_level(logging.INFO)


@pytest.mark.parametrize('engine', ['mysql', 'mariadb'])
@pytest.mark.parametrize('replica', [False, True])
def test_mysql_host_log_matches_the_selected_driver_endpoint(caplog, engine, replica):
    config = {'host': 'primary.example', 'port': 3306, 'user': 'private-login', 'password': 'private-password',
              'engine': engine}
    if replica:
        config['replica_host'] = 'replica.example'
    source = FastSyncTapMySql(config, Mock())
    with patch('pipelinewise.fastsync.commons.tap_mysql.pymysql.connect') as connect, \
            patch.object(source, 'run_session_sqls'):
        source.open_connections()
        source.open_connections()
    host = 'replica.example' if replica else 'primary.example'
    assert all(call.kwargs['host'] == host for call in connect.call_args_list)
    assert caplog.messages == [f'Connecting to MySQL/MariaDB source host: {host}']


@pytest.mark.parametrize('prioritize_primary', [False, True])
def test_postgres_host_log_matches_the_selected_driver_endpoint(caplog, prioritize_primary):
    config = {'host': 'primary.example', 'replica_host': 'replica.example', 'port': 5432,
              'user': 'private-login', 'password': 'private-password', 'dbname': 'analytics'}
    with patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect') as connect:
        connect.return_value.server_version = 150013
        FastSyncTapPostgres.get_connection(config, prioritize_primary=prioritize_primary)
        FastSyncTapPostgres.get_connection(config, prioritize_primary=prioritize_primary)
    host = 'primary.example' if prioritize_primary else 'replica.example'
    assert f"host='{host}'" in connect.call_args.args[0]
    assert [message for message in caplog.messages if message.startswith('Connecting to ')] == [
        f'Connecting to PostgreSQL source host: {host}',
    ]
    assert 'private-login' not in caplog.text and 'private-password' not in caplog.text


@pytest.mark.parametrize('srv', ['true', 'false'])
def test_mongodb_host_log_preserves_seed_list_and_hides_uri_credentials(caplog, srv):
    host = 'seed.example' if srv == 'true' else 'seed-one.example,seed-two.example'
    config = {'host': host, 'port': 27017, 'user': 'private-login', 'password': 'private-password',
              'database': 'analytics', 'auth_database': 'admin', 'srv': srv}
    source = FastSyncTapMongoDB(config, Mock())
    with patch('pipelinewise.fastsync.commons.tap_mongodb.MongoClient') as connect:
        source.open_connection()
        source.open_connection()
    assert host in connect.call_args.args[0]
    assert caplog.messages == [f'Connecting to MongoDB source host: {host}']
