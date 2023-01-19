"""
Backend database exceptions
"""


class NotConfiguredBackendDatabaseException(Exception):
    """
    Exception to raise when attempted to use a not configured backend database
    """

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class InvalidBackendDatabaseException(Exception):
    """
    Exception to raise when backend database is not configured correctly
    """

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
