"""
Pipelinewise common utils between cli and fastsync
"""
from typing import Optional


def safe_column_name(name: Optional[str], quote_character: Optional[str]='"') -> Optional[str]:
    """
    Makes column name safe by capitalizing and wrapping it in double quotes
    Args:
        name: column name
        quote_character: character the database uses for quoting identifiers

    Returns:
        str: safe column name
    """
    if name:
        return f'{quote_character}{name.upper()}{quote_character}'
    return name
