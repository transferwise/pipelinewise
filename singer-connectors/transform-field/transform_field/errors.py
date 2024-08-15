class CatalogRequiredException(Exception):
    """Raised when catalog needs to be provided but it has not been"""


class StreamNotFoundException(Exception):
    """Raised when catalog doesn't have a given selected stream"""

    def __init__(self, stream):
        message = f'Catalog doesn\'t have the selected stream `{stream}`!'

        super().__init__(message)


class NoStreamSchemaException(Exception):
    """Raised when stream has an empty schema"""

    def __init__(self, stream):
        message = f'Stream `{stream}` has an empty schema!'

        super().__init__(message)


class InvalidTransformationException(Exception):
    """Raised when the given transformation is invalid"""


class UnsupportedTransformationTypeException(Exception):
    """Raised when the given transformation type is not supported"""

    def __init__(self, trans_type):
        message = f'Transformation `{trans_type}` is not supported!'

        super().__init__(message)
