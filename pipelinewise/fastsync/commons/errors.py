class ExportError(Exception):
    """Raised when export fails"""

class TableNotFoundError(Exception):
    """Raised when configured table doesn't exist in source"""

class MongoDBInvalidDatetimeError(Exception):
    """Raised when a bson datetime is invalid and cannot be serialized"""
