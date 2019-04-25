
# Taps are implemented by different persons and teams without
# common naming convention and structures in the tap specific
# properties.json.
#
# Since PipelineWise is using common YAML configuration files across
# every supported tap we need to normalise the naming differences to
# implement common functions that work smootly with every tap.
# i.e. stream selection, transformations, etc.
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