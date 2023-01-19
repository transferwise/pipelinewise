"""
Backend database exceptions
"""


class NoSyncPeriodException(Exception):
    """
    Exception to raise when sync period is not set for the schedule
    """

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class InvalidSyncPeriodException(Exception):
    """
    Exception to raise when sync period is using invalid format
    """

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
