"""
Pipelinewise common utils between cli and fastsync
"""
import socket
from typing import Optional


def safe_column_name(
    name: Optional[str], quote_character: Optional[str] = None
) -> Optional[str]:
    """
    Makes column name safe by capitalizing and wrapping it in double quotes
    Args:
        name: column name
        quote_character: character the database uses for quoting identifiers

    Returns:
        str: safe column name
    """
    if quote_character is None:
        quote_character = '"'
    if name:
        return f'{quote_character}{name.upper()}{quote_character}'
    return name


def get_hostname() -> str:
    """
    Get the hostname of the machine
    Returns: hostname
    """
    return socket.gethostname()
