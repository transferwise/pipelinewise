
# Taps are implementing some common things differently and naming
# are not always in sync.
#
# Since PipelineWise is configured with common YAML files accross
# every supported tap we need to identify
tap_properties = {
    'tap-mysql': {
        'tap_stream_id_pattern': '{{schema_name}}-{{table_name}}',
        'tap_catalog_argument': '--properties',
    },

    'tap-postgres': {
        'tap_stream_id_pattern': '{{database_name}}-{{schema_name}}-{{table_name}}',
        'tap_catalog_argument': '--properties',
    },

    'tap-kafka': {
        'tap_stream_id_pattern': '{{table_name}}',
        'tap_catalog_argument': '--properties',
    },

    'tap-zendesk': {
        'tap_stream_id_pattern': '{{table_name}}',
        'tap_catalog_argument': '--catalog',
    },

    'tap-adwords': {
        'tap_stream_id_pattern': '{{table_name}}',
        'tap_catalog_argument': '--catalog',
    },

    # Default values to use as a fallback method
    'DEFAULT': {
        'tap_stream_id_pattern': '{{schema_name}}-{{table_name}}',
        'tap_catalog_argument': '--catalog',
    },
}