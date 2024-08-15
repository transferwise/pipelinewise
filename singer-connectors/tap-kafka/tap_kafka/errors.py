class InvalidConfigException(Exception):
    """
    Exception to raise when the config is not valid
    """
    pass


class InvalidBookmarkException(Exception):
    """
    Exception to raise when bookmark is not valid
    """
    pass


class DiscoveryException(Exception):
    """
    Exception to raise when discovery failed
    """
    pass


class InvalidTimestampException(Exception):
    """
    Exception to raise when a kafka timestamp tuple is invalid
    """
    pass


class TimestampNotAvailableException(Exception):
    """
    Exception to raise when timestamp not available in a kafka message
    """
    pass


class ProtobufCompilerException(Exception):
    """
    Exception to raise when protobuf compiler fails
    """
    pass


class AllBrokersDownException(Exception):
    """
    Exception to raise when kafka broker is not available
    """
    pass


class PrimaryKeyNotFoundException(Exception):
    """
    Exception to raise if either the custom primary or message key not found in the message
    """
    pass
