"""Compatibility tests for query-event status variables added after mysql-replication 0.46."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pymysqlreplication.event import QueryEvent

from tap_mysql.sync_strategies import binlog


@pytest.mark.parametrize('value', [0, 1])
def test_mysql_97_cascade_trigger_status_variable_is_consumed(value):
    event = SimpleNamespace(packet=Mock())
    event.packet.read_uint8.return_value = value

    QueryEvent._read_status_vars_value_for_key(event, binlog.Q_ENABLE_CASCADE_TRIGGERS)

    assert event.enable_cascade_triggers == value
    event.packet.read_uint8.assert_called_once_with()


def test_cascade_trigger_status_variable_uses_mysql_protocol_key():
    assert binlog.Q_ENABLE_CASCADE_TRIGGERS == 0x15


def test_existing_status_variables_still_use_upstream_parser():
    event = SimpleNamespace(packet=Mock())

    QueryEvent._read_status_vars_value_for_key(event, 0x0E)

    event.packet.read_uint8.assert_not_called()
    assert not hasattr(event, 'enable_cascade_triggers')


def test_cascade_trigger_status_patch_is_idempotent():
    reader = QueryEvent._read_status_vars_value_for_key

    binlog.preserve_cascade_trigger_status_var()

    assert QueryEvent._read_status_vars_value_for_key is reader
