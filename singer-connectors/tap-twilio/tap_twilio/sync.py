import math
from datetime import timedelta, datetime
from dateutil import parser

import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc, strftime

from tap_twilio.streams import STREAMS, flatten_streams
from tap_twilio.transform import transform_json

LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.error('OS Error writing schema for: %s', stream_name)
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.error('OS Error writing record for: %s', stream_name)
        LOGGER.error('Stream: %s, record: %s', stream_name, record)
        raise err
    except TypeError as err:
        LOGGER.error('Type Error writing record for: %s', stream_name)
        LOGGER.error('Stream: %s, record: %s', stream_name, record)
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return state.get('bookmarks', {}).get(stream, default)


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.debug('Write state for stream: %s, value: %s', stream, value)
    singer.write_state(state)


def transform_datetime(this_dttm):
    with Transformer() as transformer:
        # pylint: disable=protected-access
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


def process_records(catalog,  # pylint: disable=too-many-branches
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id

            # Transform record for Singer.io
            with Transformer() as transformer:
                try:
                    transformed_record = transformer.transform(
                        record,
                        schema,
                        stream_metadata)
                except Exception as err:
                    LOGGER.error('Transformer Error: %s', err)
                    raise err

                # Reset max_bookmark_value to new value if higher
                if transformed_record.get(bookmark_field):
                    if max_bookmark_value is None or \
                            transformed_record[bookmark_field] > \
                            transform_datetime(max_bookmark_value):
                        max_bookmark_value = transformed_record[bookmark_field]

                if bookmark_field and (bookmark_field in transformed_record):
                    last_dttm = transform_datetime(last_datetime)
                    bookmark_dttm = transform_datetime(transformed_record[bookmark_field])
                    # Keep only records whose bookmark is after the last_datetime
                    if bookmark_dttm:
                        if bookmark_dttm >= last_dttm:
                            write_record(stream_name, transformed_record, \
                                         time_extracted=time_extracted)
                            counter.increment()
                else:
                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        return max_bookmark_value, counter.value


def get_dates(state, stream_name, start_date, bookmark_field, bookmark_query_field_from,
              bookmark_query_field_to, date_window_days):
    """
    Given the state, stream, endpoint config, and start date, determine the date window for syncing
    as well as the relevant dates and window day increments.
    :param state: Tap sync state consisting of bookmarks, last synced stream, etc.
    :param stream_name: Stream being synced
    :param start_date: Tap config start date
    :param bookmark_field: Field for the stream used as a bookmark
    :param bookmark_query_field_from: field, if applicable, for windowing the stream request
    :param bookmark_query_field_to: field, if applicable, for windowing the stream request
    :param date_window_days: number of days to perform endpoint call for at a time, defaults to 30
    :return:
    """
    # Get the latest bookmark for the stream and set the last_integer/datetime
    last_datetime = get_bookmark(state, stream_name, start_date)
    max_bookmark_value = last_datetime
    LOGGER.debug('stream: %s, bookmark_field: %s, last_datetime: %s', stream_name, bookmark_field, last_datetime)

    # windowing: loop through date date_window_days date windows from last_datetime to now_datetime
    now_datetime = utils.now()
    if bookmark_query_field_from and bookmark_query_field_to:
        # date_window_days: Number of days in each date window
        # date_window_days from config, default = 30; passed to function from sync

        # Set start window
        last_dttm = strptime_to_utc(last_datetime)
        start_window = now_datetime
        if last_dttm < start_window:
            start_window = last_dttm

        # Set end window
        end_window = start_window + timedelta(days=date_window_days)
        end_window = min(end_window, now_datetime)
    else:
        start_window = strptime_to_utc(last_datetime)
        end_window = now_datetime
        diff_sec = (end_window - start_window).seconds
        date_window_days = math.ceil(diff_sec / (3600 * 24))  # round-up difference to days

    return start_window, end_window, date_window_days, now_datetime, last_datetime, max_bookmark_value


# Sync a specific parent or child endpoint.
# pylint: disable=too-many-statements,too-many-branches
def sync_endpoint(
        client,
        config,
        catalog,
        state,
        start_date,
        stream_name,
        path,
        endpoint_config,
        bookmark_field,
        selected_streams=None,
        date_window_days=None,
        parent=None,
        parent_id=None,
        account_sid=None):
    static_params = endpoint_config.get('params', {})
    synch_since_bookmark = endpoint_config.get('synch_since_bookmark')
    bookmark_query_field_from = endpoint_config.get('bookmark_query_field_from')
    bookmark_query_field_to = endpoint_config.get('bookmark_query_field_to')
    data_key = endpoint_config.get('data_key', 'data')
    stringified_json_keys = endpoint_config.get('stringified_json_keys')
    id_fields = endpoint_config.get('key_properties')

    start_window, end_window, date_window_days, now_datetime, last_datetime, max_bookmark_value = \
        get_dates(
            state=state,
            stream_name=stream_name,
            start_date=start_date,
            bookmark_field=bookmark_field,
            bookmark_query_field_from=bookmark_query_field_from,
            bookmark_query_field_to=bookmark_query_field_to,
            date_window_days=date_window_days
        )

    endpoint_total = 0

    # pylint: disable=too-many-nested-blocks
    while start_window < now_datetime:
        LOGGER.info('START Sync for Stream: %s%s', stream_name,
                    ', Date window from: {} to {}'.format(start_window, end_window) if bookmark_query_field_from else '')

        params = static_params  # adds in endpoint specific, sort, filter params

        if bookmark_query_field_from and bookmark_query_field_to:
            params[bookmark_query_field_from] = strftime(start_window)[:10]  # truncate date
            params[bookmark_query_field_to] = strftime(end_window)[:10]  # truncate date

        if synch_since_bookmark and bookmark_query_field_from:
            current_stream_bookmark_string = state.get('bookmarks', {}).get(stream_name)

            # if we have a bookmark for the given stream, use that one
            if current_stream_bookmark_string is not None:
                start_datetime = datetime.strptime(current_stream_bookmark_string,
                                                   '%Y-%m-%dT%H:%M:%S.%fZ')
                start_datetime_string = start_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
            # if don't, then take the start_date
            else:
                start_datetime_string = start_date
            # assign the synching param to the parameters
            params[bookmark_query_field_from] = start_datetime_string

        # pagination: loop thru all pages of data using next (if not None)
        page = 0
        api_version = endpoint_config.get('api_version')
        if api_version in path:
            if path.startswith('http'):
                next_url = path
            else:
                next_url = '{}{}'.format(endpoint_config.get('api_url'), path)
        else:
            next_url = '{}/{}/{}?Page={}&PageSize=100'.format(endpoint_config.get('api_url'), api_version, path, page)

        offset = 0
        limit = 500  # Default limit for Twilio API, unable to change this
        total_records = 0

        while next_url is not None:
            # Need URL querystring for 1st page; subsequent pages provided by next_url
            # querystring: Squash query params into string
            querystring = None
            if page == 0 and not params == {} and params is not None:
                querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
                # Replace <parent_id> in child stream params
                if parent_id:
                    querystring = querystring.replace('<parent_id>', parent_id)
            else:
                params = None
            LOGGER.debug('URL for Stream %s: %s%s',
                         stream_name,
                         next_url,
                         '?{}'.format(querystring) if params else '')

            # API request data
            data = client.get(
                url=next_url,
                path=path,
                params=querystring,
                endpoint=stream_name)

            # time_extracted: datetime when the data was extracted from the API
            time_extracted = utils.now()
            if not data or data is None or data == {}:
                total_records = 0
                break  # No data results

            # Get pagination details
            next_url = data.get('meta', {}).get('next_page_url') if endpoint_config.get(
                "pagination") == "meta" else data.get('next_page_uri')
            if next_url and not next_url.startswith('http'):
                next_url = '{}{}'.format(endpoint_config.get('api_url'), next_url)

            api_total = 0

            if not data or data is None:
                total_records = 0
                break  # No data results

            # Transform data with transform_json from transform.py
            # The data_key identifies the array/list of records below the <root> element
            transformed_data = []  # initialize the record list
            data_list = []
            data_dict = {}
            if data_key in data:
                if isinstance(data[data_key], list):
                    transformed_data = transform_json(data, data_key, stringified_json_keys)
                elif isinstance(data[data_key], dict):
                    data_list.append(data[data_key])
                    data_dict[data_key] = data_list
                    transformed_data = transform_json(data_dict, data_key, stringified_json_keys)
            else:  # data_key not in data
                if isinstance(data, list):
                    data_list = data
                    data_dict[data_key] = data_list
                    transformed_data = transform_json(data_dict, data_key, stringified_json_keys)
                elif isinstance(data, dict):
                    data_list.append(data)
                    data_dict[data_key] = data_list
                    transformed_data = transform_json(data_dict, data_key, stringified_json_keys)

            # Process records and get the max_bookmark_value and record_count for the set of records
            if stream_name in selected_streams:
                max_bookmark_value, record_count = process_records(
                    catalog=catalog,
                    stream_name=stream_name,
                    records=transformed_data,
                    time_extracted=time_extracted,
                    bookmark_field=bookmark_field,
                    max_bookmark_value=max_bookmark_value,
                    last_datetime=last_datetime,
                    parent=parent,
                    parent_id=parent_id)
                LOGGER.info('Stream %s, batch processed %s records', stream_name, record_count)
            else:
                record_count = 0

            # Loop thru parent batch records for each children objects (if should stream)
            children = endpoint_config.get('children')
            if children:
                for child_stream_name, child_endpoint_config in children.items():
                    # will this work if only grandchildren are selected
                    if child_stream_name in selected_streams:
                        LOGGER.info('START Syncing: %s', child_stream_name)
                        write_schema(catalog, child_stream_name)
                        # For each parent record
                        for record in transformed_data:
                            i = 0
                            # Set parent_id
                            for id_field in id_fields:
                                if i == 0:
                                    parent_id_field = id_field
                                if id_field == 'id':
                                    parent_id_field = id_field
                                i = i + 1
                            parent_id = record.get(parent_id_field)

                            # sync_endpoint for child
                            LOGGER.info(
                                'START Sync for Stream: %s, parent_stream: %s, parent_id: %s', child_stream_name,
                                stream_name, parent_id)

                            # If the results of the stream being synced has child streams,
                            # their endpoints will be in the results,
                            # this will grab the child path for the child stream we're syncing,
                            # if we're syncing it. If it doesn't exist we just skip it below.
                            child_paths = record.get('_subresource_uris', record.get('links', {}))

                            # For some resources, the name of the stream is the key for the child_paths
                            # but for others', the data_key contains the correct key
                            child_key = child_endpoint_config.get('parent_subresource_key',
                                                                  child_endpoint_config.get('data_key'))

                            child_path = child_paths.get(child_stream_name,
                                                         child_paths.get(child_key))

                            child_bookmark_field = next(iter(child_endpoint_config.get(
                                'replication_keys', [])), None)

                            if child_path:
                                child_total_records = sync_endpoint(
                                    client=client,
                                    config=config,
                                    catalog=catalog,
                                    state=state,
                                    start_date=start_date,
                                    stream_name=child_stream_name,
                                    path=child_path,
                                    endpoint_config=child_endpoint_config,
                                    bookmark_field=child_bookmark_field,
                                    selected_streams=selected_streams,
                                    # The child endpoint may be an endpoint that needs to window
                                    # so we'll re-pull from the config here (or pass in the default)
                                    date_window_days=int(config.get('date_window_days', '30')),
                                    parent=child_endpoint_config.get('parent'),
                                    parent_id=parent_id,
                                    account_sid=account_sid)
                            else:
                                LOGGER.info(
                                    'No child stream %s for parent stream %s in subresource uris',
                                    child_stream_name, stream_name)
                                child_total_records = 0
                            LOGGER.info('FINISHED Sync for Stream: %s, parent_id: %s, total_records: %s',
                                        child_stream_name,
                                        parent_id, child_total_records)
                            # End transformed data record loop
                        # End if child in selected streams
                    # End child streams for parent
                # End if children

            # Parent record batch
            # Adjust total_records w/ record_count, if needed
            if record_count > total_records:
                total_records = total_records + record_count
            else:
                total_records = api_total

            # to_rec: to record; ending record for the batch page
            to_rec = min(offset + limit, total_records)

            LOGGER.info('Synced Stream: %s, page: %s, %s to %s of total records: %s',
                        stream_name,
                        page,
                        offset,
                        to_rec,
                        total_records)
            # Pagination: increment the offset by the limit (batch-size) and page
            offset = offset + limit
            page = page + 1
            # End page/batch - while next URL loop

        # Update the state with the sooner of now_datetime or max_bookmark_value
        # If we synced any entities which were created during the synchronization process, we save the start of the
        # sync as bookmark, if all we synced was entities from before we started, we save the latest entity
        # Twilio API does not allow page/batch sorting; bookmark written for date window
        if bookmark_field:
            max_bookmark_date_time = parser.parse(max_bookmark_value)
            new_bookmark_date_time = sooner_date_time(max_bookmark_date_time, now_datetime)
            write_bookmark(state, stream_name, new_bookmark_date_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))

        # Increment date window and sum endpoint_total
        start_window = end_window
        next_end_window = end_window + timedelta(days=date_window_days)
        if next_end_window > now_datetime:
            end_window = now_datetime
        else:
            end_window = next_end_window
        endpoint_total = endpoint_total + total_records
        # End date window

    # Return total_records (for all pages and date windows)
    return endpoint_total


def sooner_date_time(date_time_1, date_time_2):
    if date_time_1 < date_time_2:
        return date_time_1
    return date_time_2


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


def sync(client, config, catalog, state):
    if 'start_date' in config:
        start_date = config['start_date']

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: %s', last_stream)
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: %s', selected_streams)

    # Get lists of parent and child streams to sync (from streams.py and catalog)
    # For children, ensure that dependent parent_stream is included
    parent_streams = []
    child_streams = []
    # Get all streams (parent + child) from streams.py
    flat_streams = flatten_streams()
    # Loop thru all streams
    for stream_name, stream_metadata in flat_streams.items():
        # If stream has a parent_stream, then it is a child stream
        parent_stream = stream_metadata.get('parent_stream')
        # Append selected parent streams
        if parent_stream not in selected_streams and stream_name in selected_streams:
            parent_streams.append(parent_stream)
            child_streams.append(stream_name)
        # Append selected child streams
        elif parent_stream and stream_name in selected_streams:
            parent_streams.append(stream_name)
            child_streams.append(stream_name)
            # Append un-selected parent streams of selected children
            if parent_stream not in selected_streams:
                parent_streams.append(parent_stream)
    LOGGER.info('Sync Parent Streams: %s', parent_streams)
    LOGGER.info('Sync Child Streams: %s', child_streams)

    if not selected_streams or selected_streams == []:
        return

    # Loop through endpoints in selected_streams
    for stream_name, endpoint_config in STREAMS.items():
        required_streams = list(set(parent_streams + child_streams))
        if stream_name in required_streams:
            LOGGER.info('START Syncing: %s', stream_name)
            write_schema(catalog, stream_name)
            update_currently_syncing(state, stream_name)
            path = endpoint_config.get('path', stream_name)
            bookmark_field = next(iter(endpoint_config.get('replication_keys', [])), None)
            total_records = sync_endpoint(
                client=client,
                config=config,
                catalog=catalog,
                state=state,
                start_date=start_date,
                stream_name=stream_name,
                path=path,
                endpoint_config=endpoint_config,
                bookmark_field=bookmark_field,
                selected_streams=selected_streams,
                date_window_days=int(config.get('date_window_days', '30')))

            update_currently_syncing(state, None)
            LOGGER.info('FINISHED Syncing: %s, total_records: %s', stream_name, total_records)
