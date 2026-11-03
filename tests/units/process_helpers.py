"""Helpers for tests that start and stop real processes."""

import time

import psutil


def wait_until_gone(pid, timeout=5):
    """Return whether pid exited; orphans may stay zombies when PID 1 does not reap them."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                return True
        except psutil.NoSuchProcess:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.05)
