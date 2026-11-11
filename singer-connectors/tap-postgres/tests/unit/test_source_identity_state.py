from unittest.mock import patch

import tap_postgres


def test_logical_sync_keeps_source_identity_while_pruning_deselected_bookmarks():
    """PipelineWise binds tap state to its source; the logical phase must carry that binding forward."""
    identity = {'system_identifier': '7300000000000000001', 'timeline_id': 2}
    state = {
        'currently_syncing': None,
        'source_identity': identity,
        'bookmarks': {
            'public-selected': {'last_replication_method': 'LOG_BASED', 'lsn': 10},
            'public-deselected': {'last_replication_method': 'LOG_BASED', 'lsn': 5},
        },
    }
    stream = {'tap_stream_id': 'public-selected', 'schema': {'properties': {}}, 'metadata': []}

    with patch.object(tap_postgres.logical_replication, 'sync_tables', side_effect=lambda *args, **_: args[2]):
        result = tap_postgres.sync_logical_streams({}, [stream], state, 20, 'state.json')

    assert result == {
        'currently_syncing': None,
        'source_identity': identity,
        'bookmarks': {'public-selected': {'last_replication_method': 'LOG_BASED', 'lsn': 10}},
    }
