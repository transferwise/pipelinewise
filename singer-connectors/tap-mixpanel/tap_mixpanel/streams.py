# streams: API URL endpoints to be called
# properties:
#   <root node>: Plural stream name for the endpoint
#   path: API endpoint relative path, when added to the base URL, creates the full path,
#       default = stream_name
#   key_properties: Primary key fields for identifying an endpoint record.
#   replication_method: INCREMENTAL or FULL_TABLE
#   replication_keys: bookmark_field(s), typically a date-time, used for filtering the results
#        and setting the state
#   params: Query, sort, and other endpoint specific parameters; default = {}
#   data_key: JSON element containing the results list for the endpoint
#   bookmark_query_field_from/to: From date-time field used for filtering the query
#   api_method: GET or POST
#   parent_path, parent_id_field: Used for listing parent IDs and looping through each
#   date_dictionary: True or False, to transform date keys to list-array
#   pagination: True or False, if endpoint supports pagination looping

STREAMS = {
    'export': {
        'url': 'https://data.mixpanel.com/api/2.0',
        'path': 'export',
        'data_key': 'results',
        'api_method': 'GET',
        'key_properties': ['mp_reserved_insert_id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['time'],
        'bookmark_query_field_from': 'from_date',
        'bookmark_query_field_to': 'to_date',
        'date_dictionary': False,
        'pagination': False,
        'params': {}
    },

    'engage': {
        'url': 'https://mixpanel.com/api/2.0',
        'path': 'engage',
        'data_key': 'results',
        'api_method': 'GET',
        'key_properties': ['distinct_id'],
        'replication_method': 'FULL_TABLE',
        'date_dictionary': False,
        'pagination': True,
        'params': {}
    },

    'funnels': {
        'url': 'https://mixpanel.com/api/2.0',
        'parent_path': 'funnels/list',
        'parent_id_field': 'funnel_id',
        'path': 'funnels',
        'data_key': 'data',
        'api_method': 'GET',
        'key_properties': ['funnel_id', 'date'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['datetime'],
        'bookmark_query_field_from': 'from_date',
        'bookmark_query_field_to': 'to_date',
        'date_dictionary': True,
        'pagination': False,
        'params': {
            'funnel_id': '[parent_id]',
            'unit': 'day'
        }
    },

    'cohorts': {
        'url': 'https://mixpanel.com/api/2.0',
        'path': 'cohorts/list',
        'data_key': '.',
        'api_method': 'GET',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'date_dictionary': False,
        'pagination': False,
        'params': {}
    },

    'cohort_members': {
        'url': 'https://mixpanel.com/api/2.0',
        'parent_path': 'cohorts/list',
        'parent_id_field': 'id',
        'path': 'engage',
        'data_key': 'results',
        'api_method': 'GET',
        'key_properties': ['cohort_id', 'distinct_id'],
        'replication_method': 'FULL_TABLE',
        'date_dictionary': False,
        'pagination': True,
        'params': {
            'filter_by_cohort': '{"id": [parent_id]}'
        }
    },

    'revenue': {
        'url': 'https://mixpanel.com/api/2.0',
        'path': 'engage/revenue',
        'data_key': 'results',
        'api_method': 'GET',
        'key_properties': ['date'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['datetime'],
        'bookmark_query_field_from': 'from_date',
        'bookmark_query_field_to': 'to_date',
        'date_dictionary': True,
        'pagination': False,
        'params': {
            'unit': 'day'
        }
    },

    'annotations': {
        'url': 'https://mixpanel.com/api/2.0',
        'path': 'annotations',
        'data_key': 'annotations',
        'api_method': 'GET',
        'key_properties': ['date'],
        'replication_method': 'FULL_TABLE',
        'bookmark_query_field_from': 'from_date',
        'bookmark_query_field_to': 'to_date',
        'date_dictionary': False,
        'pagination': False,
        'params': {}
    }

}
