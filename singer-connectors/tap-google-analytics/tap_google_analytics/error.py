class TapGaApiError(Exception):
    """Base exception for API errors."""

class TapGaInvalidArgumentError(TapGaApiError):
    """Exception for errors on the report definition."""

class TapGaAuthenticationError(TapGaApiError):
    """Exception for UNAUTHENTICATED && PERMISSION_DENIED errors."""

class TapGaRateLimitError(TapGaApiError):
    """Exception for Rate Limit errors."""

class TapGaQuotaExceededError(TapGaApiError):
    """Exception for Quota Exceeded errors."""

class TapGaBackendServerError(TapGaApiError):
    """Exception for 500 and 503 backend errors that are Google's fault"""

class TapGaUnknownError(TapGaApiError):
    """Exception for unknown errors."""
