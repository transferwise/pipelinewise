from datetime import timedelta, datetime, timezone
import math
import json
import pytz
import urllib
import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc
from tap_mixpanel.transform import transform_record
from tap_mixpanel.streams import STREAMS


LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.error('OS Error writing schema for: {}'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.error('OS Error writing record for: {}'.format(stream_name))
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)


def transform_datetime(this_dttm):
    with Transformer() as transformer:
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


def process_records(catalog, #pylint: disable=too-many-branches
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    max_bookmark_value=None,
                    last_datetime=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # Transform record for Singer.io
            with Transformer() as transformer:
                try:
                    transformed_record = transformer.transform(
                        record,
                        schema,
                        stream_metadata)
                except Exception as err:
                    LOGGER.error('Error: {}'.format(err))
                    LOGGER.error(' for schema: {}'.format(json.dumps(
                        schema, sort_keys=True, indent=2)))
                    raise err

                # Reset max_bookmark_value to new value if higher
                if transformed_record.get(bookmark_field):
                    if max_bookmark_value is None or \
                        transformed_record[bookmark_field] > transform_datetime(max_bookmark_value):
                        max_bookmark_value = transformed_record[bookmark_field]

                if bookmark_field and (bookmark_field in transformed_record):
                    last_dttm = transform_datetime(last_datetime)
                    bookmark_dttm = transform_datetime(transformed_record[bookmark_field])
                    # Keep only records whose bookmark is after the last_datetime
                    if bookmark_dttm >= last_dttm:
                        write_record(stream_name, transformed_record, \
                            time_extracted=time_extracted)
                        counter.increment()
                else:
                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        return max_bookmark_value, counter.value


# Sync a specific endpoint
def sync_endpoint(client, #pylint: disable=too-many-branches
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  path,
                  endpoint_config,
                  bookmark_field=None,
                  project_timezone=None,
                  days_interval=None,
                  attribution_window=None,
                  export_events=None,
                  denest_properties_flag=None):

    # Get endpoint_config fields
    url = endpoint_config.get('url')
    data_key = endpoint_config.get('data_key', 'results')
    api_method = endpoint_config.get('api_method')
    parent_path = endpoint_config.get('parent_path')
    parent_id_field = endpoint_config.get('parent_id_field')
    static_params = endpoint_config.get('params', {})
    bookmark_query_field_from = endpoint_config.get('bookmark_query_field_from')
    bookmark_query_field_to = endpoint_config.get('bookmark_query_field_to')
    id_fields = endpoint_config.get('key_properties')
    date_dictionary = endpoint_config.get('date_dictionary', False)
    pagination = endpoint_config.get('pagination', False)

    # Get the latest bookmark for the stream and set the last_integer/datetime
    last_datetime = None
    max_bookmark_value = None
    last_datetime = get_bookmark(state, stream_name, start_date)
    max_bookmark_value = last_datetime

    write_schema(catalog, stream_name)

    # windowing: loop through date days_interval date windows from last_datetime to now_datetime
    tzone = pytz.timezone(project_timezone)
    now_datetime = datetime.now(tzone)

    if bookmark_query_field_from and bookmark_query_field_to:
        # days_interval from config date_window_size, default = 60; passed to function from sync
        if not days_interval:
            days_interval = 30

        last_dttm = strptime_to_utc(last_datetime)
        delta_days = (now_datetime - last_dttm).days
        if delta_days <= attribution_window:
            delta_days = attribution_window
            LOGGER.info("Start bookmark less than {} day attribution window.".format(
                attribution_window))
        elif delta_days >= 365:
            delta_days = 365
            LOGGER.warning("WARNING: Start date or bookmark greater than 1 year maxiumum.")
            LOGGER.warning("WARNING: Setting bookmark start to 1 year ago.")

        start_window = now_datetime - timedelta(days=delta_days)
        end_window = start_window + timedelta(days=days_interval)
        end_window = min(end_window, now_datetime)

    else:
        start_window = strptime_to_utc(last_datetime)
        end_window = now_datetime
        diff_sec = (end_window - start_window).seconds
        days_interval = math.ceil(diff_sec / (3600 * 24)) # round-up difference to days

    # LOOP order: Date Windows, Parent IDs, Page
    # Initialize counter
    endpoint_total = 0 # Total for ALL: parents, date windows, and pages

    # Begin date windowing loop
    while start_window < now_datetime:
        # Initialize counters
        date_total = 0 # Total records for a date window
        parent_total = 0 # Total records for parent ID
        total_records = 0 # Total records for all pages
        record_count = 0 # Total processed for page

        params = static_params # adds in endpoint specific, sort, filter params

        if bookmark_query_field_from and bookmark_query_field_to:
            # Request dates need to be normalized to project timezone or else errors may occur
            # Errors occur when from_date is > 365 days ago
            #   and when to_date > today (in project timezone)
            from_date = '{}'.format(start_window.astimezone(tzone))[0:10]
            to_date = '{}'.format(end_window.astimezone(tzone))[0:10]
            LOGGER.info('START Sync for Stream: {}{}'.format(
                stream_name,
                ', Date window from: {} to {}'.format(from_date, to_date) \
                    if bookmark_query_field_from else ''))
            params[bookmark_query_field_from] = from_date
            params[bookmark_query_field_to] = to_date

        # funnels and cohorts have a parent endpoint with parent_data and parent_id_field
        if parent_path and parent_id_field:
            # API request data
            LOGGER.info('URL for Parent Stream {}: {}/{}'.format(
                stream_name,
                url,
                parent_path))
            parent_data = client.request(
                method='GET',
                url=url,
                path=parent_path,
                endpoint='parent_data')
        # Other endpoints (not funnels, cohorts): Simulate parent_data with single record
        else:
            parent_data = [{'id': 'none'}]
            parent_id_field = 'id'

        for parent_record in parent_data:
            parent_id = parent_record.get(parent_id_field)
            LOGGER.info('START: Stream: {}, parent_id: {}'.format(stream_name, parent_id))

            # pagination: loop thru all pages of data using next (if not None)
            page = 0 # First page is page=0, second page is page=1, ...
            offset = 0
            limit = 250 # Default page_size
            # Initialize counters
            parent_total = 0 # Total records for parent ID
            total_records = 0 # Total records for all pages
            record_count = 0 # Total processed for page

            session_id = 'initial'
            if pagination:
                params['page_size'] = limit

            while offset <= total_records and session_id is not None:
                if pagination and page != 0:
                    params['session_id'] = session_id
                    params['page'] = page

                # querystring: Squash query params into string and replace [parent_id]
                querystring = '&'.join(['%s=%s' % (key, value) for (key, value) \
                    in params.items()]).replace(
                        '[parent_id]', str(parent_id))

                if stream_name == 'export' and export_events:
                    event = json.dumps([export_events] if isinstance(export_events, str) else export_events)
                    url_encoded = urllib.parse.quote(event)
                    querystring += f'&event={url_encoded}'

                full_url = '{}/{}{}'.format(
                    url,
                    path,
                    '?{}'.format(querystring) if querystring else '')

                LOGGER.info('URL for Stream {}: {}'.format(stream_name, full_url))

                # API request data
                data = {}

                # Export has a streaming api call
                if stream_name == 'export':
                    data = client.request_export(
                        method=api_method,
                        url=url,
                        path=path,
                        params=querystring,
                        endpoint=stream_name)

                    # time_extracted: datetime when the data was extracted from the API
                    time_extracted = utils.now()
                    transformed_data = []
                    for record in data:
                        if record and str(record) != '':
                            # transform reocord and append to transformed_data array
                            transformed_record = transform_record(record, stream_name, \
                                project_timezone, denest_properties_flag)
                            transformed_data.append(transformed_record)

                            # Check for missing keys
                            for key in id_fields:
                                val = transformed_record.get(key)
                                if val == '' or not val:
                                    LOGGER.error('Error: Missing Key')
                                    raise 'Missing Key'

                            if len(transformed_data) == limit:
                                # Process full batch (limit = 250) records
                                #   and get the max_bookmark_value and record_count
                                max_bookmark_value, record_count = process_records(
                                    catalog=catalog,
                                    stream_name=stream_name,
                                    records=transformed_data,
                                    time_extracted=time_extracted,
                                    bookmark_field=bookmark_field,
                                    max_bookmark_value=max_bookmark_value,
                                    last_datetime=last_datetime)
                                total_records = total_records + record_count
                                parent_total = parent_total + record_count
                                date_total = date_total + record_count
                                endpoint_total = endpoint_total + record_count
                                transformed_data = []

                                LOGGER.info('Stream {}, batch processed {} records, total {}, max bookmark {}'.format(
                                    stream_name,
                                    record_count,
                                    endpoint_total,
                                    max_bookmark_value))
                                # End if (batch = limit 250)
                            # End if record
                        # End has export_data records loop

                    # Process remaining, partial batch
                    if len(transformed_data) > 0:
                        max_bookmark_value, record_count = process_records(
                            catalog=catalog,
                            stream_name=stream_name,
                            records=transformed_data,
                            time_extracted=time_extracted,
                            bookmark_field=bookmark_field,
                            max_bookmark_value=max_bookmark_value,
                            last_datetime=last_datetime)
                        LOGGER.info('Stream {}, batch processed {} records'.format(
                            stream_name, record_count))

                        total_records = total_records + record_count
                        parent_total = parent_total + record_count
                        date_total = date_total + record_count
                        endpoint_total = endpoint_total + record_count
                        # End if transformed_data

                    # Export does not provide pagination; session_id = None breaks out of loop.
                    session_id = None
                    # End export stream API call

                else: # stream_name != 'export`
                    data = client.request(
                        method=api_method,
                        url=url,
                        path=path,
                        params=querystring,
                        endpoint=stream_name)

                    # time_extracted: datetime when the data was extracted from the API
                    time_extracted = utils.now()
                    if not data or data is None or data == {} or data == []:
                        LOGGER.info('No data for URL: {}'.format(full_url))
                        # No data results
                    else: # has data
                        # Transform data with transform_json from transform.py
                        # The data_key identifies the array/list of records below the <root> element
                        # LOGGER.info('data = {}'.format(data)) # TESTING, comment out
                        transformed_data = [] # initialize the record list

                        # Endpoints: funnels, revenue return results as dictionary for each date
                        # Standardize results to a list/array
                        if date_dictionary and data_key in data:
                            results = {}
                            results_list = []
                            for key, val in data[data_key].items():
                                # skip $overall summary
                                if key != '$overall':
                                    val['date'] = key
                                    val['datetime'] = '{}T00:00:00Z'.format(key)
                                    results_list.append(val)
                            results[data_key] = results_list
                            data = results

                        # Cohorts endpoint returns results as a list/array (no data_key)
                        # All other endpoints have a data_key
                        if data_key is None or data_key == '.':
                            data_key = 'results'
                            new_data = {
                                'results': data
                            }
                            data = new_data

                        transformed_data = []
                        # Loop through result records
                        for record in data[data_key]:
                            # transform reocord and append to transformed_data array
                            transformed_record = transform_record(
                                record, stream_name, project_timezone, parent_record)
                            transformed_data.append(transformed_record)

                            # Check for missing keys
                            for key in id_fields:
                                val = transformed_record.get(key)
                                if val == '' or not val:
                                    LOGGER.error('Error: Missing Key')
                                    raise 'Missing Key'

                            # End data record loop

                        if not transformed_data or transformed_data is None or \
                            transformed_data == []:
                            LOGGER.info('No transformed data for data = {}'.format(data))
                            # No transformed data results
                        else: # has transformed data
                            # Process records and get the max_bookmark_value and record_count
                            max_bookmark_value, record_count = process_records(
                                catalog=catalog,
                                stream_name=stream_name,
                                records=transformed_data,
                                time_extracted=time_extracted,
                                bookmark_field=bookmark_field,
                                max_bookmark_value=max_bookmark_value,
                                last_datetime=last_datetime)
                            LOGGER.info('Stream {}, batch processed {} records'.format(
                                stream_name, record_count))

                            # set total_records and pagination fields
                            if page == 0:
                                if isinstance(data, dict):
                                    total_records = data.get('total', record_count)
                                else:
                                    total_records = record_count
                            parent_total = parent_total + record_count
                            date_total = date_total + record_count
                            endpoint_total = endpoint_total + record_count
                            if isinstance(data, dict):
                                session_id = data.get('session_id', None)

                            # to_rec: to record; ending record for the batch page
                            if pagination:
                                to_rec = min(offset + limit, total_records)
                            else:
                                to_rec = record_count

                            LOGGER.info('Synced Stream: {}, page: {}, {} to {} of total: {}'.format(
                                stream_name,
                                page,
                                offset,
                                to_rec,
                                total_records))
                            # End has transformed data
                        # End has data results

                    # Pagination: increment the offset by the limit (batch-size) and page
                    offset = offset + limit
                    page = page + 1
                    # End page/batch loop
                # End stream != 'export'
            LOGGER.info('FINISHED: Stream: {}, parent_id: {}'.format(stream_name, parent_id))
            LOGGER.info('  Total records for parent: {}'.format(parent_total))
            # End parent record loop

        LOGGER.info('FINISHED Sync for Stream: {}{}'.format(
            stream_name,
            ', Date window from: {} to {}'.format(from_date, to_date) \
                if bookmark_query_field_from else ''))
        LOGGER.info('  Total records for date window: {}'.format(date_total))
        # Increment date window
        start_window = end_window
        next_end_window = end_window + timedelta(days=days_interval)
        if next_end_window > now_datetime:
            end_window = now_datetime
        else:
            end_window = next_end_window

        # Update the state with the max_bookmark_value for the stream
        if bookmark_field:
            write_bookmark(state, stream_name, max_bookmark_value)

        # End date window loop

    # Return endpoint_total across all batches
    return endpoint_total


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client, config, catalog, state, start_date):
    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams:
        return

    # Loop through selected_streams
    for stream_name in selected_streams:
        LOGGER.info('START Syncing: {}'.format(stream_name))
        update_currently_syncing(state, stream_name)
        endpoint_config = STREAMS[stream_name]
        path = endpoint_config.get('path', stream_name)
        bookmark_field = next(iter(endpoint_config.get('replication_keys', [])), None)
        endpoint_total = sync_endpoint(
            client=client,
            catalog=catalog,
            state=state,
            start_date=start_date,
            stream_name=stream_name,
            path=path,
            endpoint_config=endpoint_config,
            bookmark_field=bookmark_field,
            project_timezone=config.get('project_timezone', 'UTC'),
            days_interval=int(config.get('date_window_size', '30')),
            attribution_window=int(config.get('attribution_window', '5')),
            export_events=config.get('export_events'),
            denest_properties_flag=config.get('denest_properties', 'true')
        )

        update_currently_syncing(state, None)
        LOGGER.info('FINISHED Syncing: {}, Total endpoint records: {}'.format(
            stream_name,
            endpoint_total))
