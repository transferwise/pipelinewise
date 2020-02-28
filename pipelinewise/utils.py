"""
Pipelinewise common utils between cli and fastsync
"""

def safe_column_name(name: str) -> str:
    """
    Makes column name safe by capitalizing and wrapping it in double quotes
    Args:
        name: column name

    Returns:
        str: safe column name
    """
    return f'"{name.upper()}"'
