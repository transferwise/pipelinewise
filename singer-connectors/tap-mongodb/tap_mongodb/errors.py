class InvalidReplicationMethodException(Exception):
    """Exception for errors related to replication methods"""

    def __init__(self, replication_method, message=None):
        msg = f"Invalid replication method {replication_method}!"

        if message is not None:
            msg = f'{msg} {message}'

        super().__init__(msg)


class UnsupportedKeyTypeException(Exception):
    """Raised if key type is unsupported"""

class MongoAssertionException(Exception):
    """Raised if Mongo exhibits incorrect behavior"""

class MongoInvalidDateTimeException(Exception):
    """Raised if we find an invalid date-time that we can't handle"""

class SyncException(Exception):
    """Raised if we find an invalid date-time that we can't handle"""

class NoReadPrivilegeException(Exception):
    """Raised if the DB user has no read privilege on the DB"""
    def __init__(self, user, db_name):
        msg = f"The user '{user}' has no read privilege on the database '{db_name}'!"
        super().__init__(msg)

class InvalidUpdateBufferSizeError(Exception):
    """Raised if the given update buffer size used in log_based is invalid"""
    def __init__(self, size, reason):
        msg = f"Invalid update buffer size {size}! {reason}"
        super().__init__(msg)

class InvalidAwaitTimeError(Exception):
    """Raised if the given await time used in log_based is invalid"""
    def __init__(self, time_ms, reason):
        msg = f"Invalid await time {time_ms}! {reason}"
        super().__init__(msg)
