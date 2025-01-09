"""
Tap configuration related stuff
"""
from voluptuous import Schema, Required, Optional

CONFIG_CONTRACT = Schema([{
    Required('table_name'): str,
    Required('search_pattern'): str,
    Optional('key_properties'): [str],
    Optional('search_prefix'): str,
    Optional('date_overrides'): [str],
    Optional('delimiter'): str
}])
