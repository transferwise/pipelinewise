from unittest.mock import patch

import pytest

from pipelinewise.cli import source_identity


@pytest.fixture(autouse=True)
def no_source_identity_connection(request):
    """Keep unit tests off real sources: the sample LOG_BASED taps point at localhost."""
    if request.node.get_closest_marker('real_source_identity'):
        yield
        return
    with patch.object(source_identity, 'read_source_identity', return_value=None):
        yield
