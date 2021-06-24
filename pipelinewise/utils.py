"""
Pipelinewise common utils between cli and fastsync
"""
from typing import Optional


def safe_column_name(name: Optional[str], quote_character: Optional[str]='"') -> Optional[str]:
    if name:
        return f'{quote_character}{name.upper()}{quote_character}'
    return name
