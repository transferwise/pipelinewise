import signal

import pytest


@pytest.fixture(autouse=True)
def restore_termination_signal_handlers():
    """Keep handlers installed by mocked source connections from leaking into later tests."""
    previous = {signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)}
    yield
    for signum, handler in previous.items():
        signal.signal(signum, handler)
