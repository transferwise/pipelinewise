"""
PipelineWise CLI - Alert handler exceptions
"""


class NotImplementedAlertHandlerException(Exception):
    """
    Exception to raise when attempted to use a not implemented alert handler class
    """
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class NotConfiguredAlertHandlerException(Exception):
    """
    Exception to raise when attempted to use a not configured alert handler
    """
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class InvalidAlertHandlerException(Exception):
    """
    Exception to raise when alert handler not configured correctly
    """
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
