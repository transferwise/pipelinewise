"""
Pipelinewise common utils between cli and fastsync
"""
from typing import Optional


def safe_column_name(name: Optional[str]) -> Optional[str]:
    """
    Makes column name safe by capitalizing and wrapping it in double quotes
    Args:
        name: column name

    Returns:
        str: safe column name
    """
    if name:
        return f'"{name.upper()}"'

    return name