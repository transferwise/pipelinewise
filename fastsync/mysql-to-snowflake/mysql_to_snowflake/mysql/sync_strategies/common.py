#!/usr/bin/env python3
# pylint: disable=too-many-arguments,duplicate-code,too-many-locals

import copy
import datetime
import singer
import time
import csv
import os
import gzip
import boto3

import singer.metrics as metrics
from singer import metadata
from singer import utils

LOGGER = singer.get_logger()

def escape(string):
    if '`' in string:
        raise Exception("Can't escape identifier {} because it contains a backtick"
                        .format(string))
    return '`' + string + '`'


def generate_tap_stream_id(table_schema, table_name):
    return table_schema + '-' + table_name

def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version


def stream_is_selected(stream):
    md_map = metadata.to_map(stream.metadata)
    selected_md = metadata.get(md_map, (), 'selected')

    return selected_md


def property_is_selected(stream, property_name):
    md_map = metadata.to_map(stream.metadata)
    return singer.should_sync_field(
        metadata.get(md_map, ('properties', property_name), 'inclusion'),
        metadata.get(md_map, ('properties', property_name), 'selected'),
        True)


def get_is_view(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('is-view')


def get_database_name(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('database-name')


def get_key_properties(catalog_entry):
    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    is_view = get_is_view(catalog_entry)

    if is_view:
        key_properties = stream_metadata.get('view-key-properties', [])
    else:
        key_properties = stream_metadata.get('table-key-properties', [])

    return key_properties


def column_with_transformation(table, column, transforms):
    escaped_column = escape(column)
    default_transform = "REPLACE({}, '\\n', ' ') AS {}".format(escaped_column, escaped_column)

    if transforms:
        transform = next((t for t in transforms if t['stream'] == table and t['fieldId'] == column), None)
        if transform:
            transform_type = transform.get('type')
            if transform_type == 'SET-NULL':
                return 'NULL AS {}'.format(escaped_column)
            elif transform_type == 'HASH':
                return 'SHA2({}, 256) AS {}'.format(escaped_column, escaped_column)
            elif 'HASH-SKIP-FIRST' in transform_type:
                skip_first_n = transform_type[-1]
                return 'CONCAT(SUBSTRING({}, 1, {}), SHA2(SUBSTRING({}, {} + 1), 256))'.format(escaped_column, skip_first_n, escaped_column, skip_first_n)
            elif transform_type == 'MASK-DATE':
                return "DATE_FORMAT({}, '%Y-01-01') AS {}".format(escaped_column, escaped_column)
            elif transform_type == 'MASK-NUMBER':
                return '0 AS {}'.format(escaped_column)
    
    return default_transform

def generate_select_sql(catalog_entry, columns, transforms):
    database_name = get_database_name(catalog_entry)
    escaped_db = escape(database_name)
    escaped_table = escape(catalog_entry.table)
    escaped_columns = [column_with_transformation(catalog_entry.table, c, transforms) for c in columns]

    select_sql = 'SELECT {} FROM {}.{}'.format(
        ','.join(escaped_columns),
        escaped_db,
        escaped_table)

    return select_sql


def row_to_singer_record(catalog_entry, version, row, columns, time_extracted):
    row_to_persist = ()
    for idx, elem in enumerate(row):
        property_type = catalog_entry.schema.properties[columns[idx]].type
        if isinstance(elem, datetime.datetime):
            row_to_persist += (elem.isoformat() + '+00:00',)

        elif isinstance(elem, datetime.date):
            row_to_persist += (elem.isoformat() + 'T00:00:00+00:00',)

        elif isinstance(elem, datetime.timedelta):
            epoch = datetime.datetime.utcfromtimestamp(0)
            timedelta_from_epoch = epoch + elem
            row_to_persist += (timedelta_from_epoch.isoformat() + '+00:00',)

        elif isinstance(elem, bytes):
            # for BIT value, treat 0 as False and anything else as True
            boolean_representation = elem != b'\x00'
            row_to_persist += (boolean_representation,)

        elif 'boolean' in property_type or property_type == 'boolean':
            if elem is None:
                boolean_representation = None
            elif elem == 0:
                boolean_representation = False
            else:
                boolean_representation = True
            row_to_persist += (boolean_representation,)

        else:
            row_to_persist += (elem,)
    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=catalog_entry.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)


def whitelist_bookmark_keys(bookmark_key_set, tap_stream_id, state):
    for bk in [non_whitelisted_bookmark_key
               for non_whitelisted_bookmark_key
               in state.get('bookmarks', {}).get(tap_stream_id, {}).keys()
               if non_whitelisted_bookmark_key not in bookmark_key_set]:
        singer.clear_bookmark(state, tap_stream_id, bk)


def sync_query(cursor, catalog_entry, state, select_sql, columns, stream_version, params):
    replication_key = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'replication_key')

    query_string = cursor.mogrify(select_sql, params)

    time_extracted = utils.now()

    LOGGER.info('Running %s', query_string)
    cursor.execute(select_sql, params)

    row = cursor.fetchone()
    rows_saved = 0

    database_name = get_database_name(catalog_entry)

    with metrics.record_counter(None) as counter:
        counter.tags['database'] = database_name
        counter.tags['table'] = catalog_entry.table

        while row:
            counter.increment()
            rows_saved += 1
            record_message = row_to_singer_record(catalog_entry,
                                                  stream_version,
                                                  row,
                                                  columns,
                                                  time_extracted)
            singer.write_message(record_message)

            md_map = metadata.to_map(catalog_entry.metadata)
            stream_metadata = md_map.get((), {})
            replication_method = stream_metadata.get('replication-method')

            if replication_method in {'FULL_TABLE', 'LOG_BASED'}:
                key_properties = get_key_properties(catalog_entry)

                max_pk_values = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'max_pk_values')

                if max_pk_values:
                    last_pk_fetched = {k:v for k,v in record_message.record.items()
                                       if k in key_properties}

                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'last_pk_fetched',
                                                  last_pk_fetched)

            elif replication_method == 'INCREMENTAL':
                if replication_key is not None:
                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'replication_key',
                                                  replication_key)

                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'replication_key_value',
                                                  record_message.record[replication_key])
            if rows_saved % 1000 == 0:
                singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

            row = cursor.fetchone()

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def export_query(cursor, catalog_entry, state, select_sql, columns, stream_version, params, args):
    query_string = cursor.mogrify(select_sql, params)
    LOGGER.info('Running %s', query_string)

    cursor.execute(select_sql, params)

    row = cursor.fetchone()
    rows_saved = 0

    database_name = get_database_name(catalog_entry)
    table_name = catalog_entry.table
    filename = "pipelinewise_{}_{}_{}.csv.gz".format(database_name, table_name, time.strftime("%Y%m%d-%H%M%S"))
    path = os.path.join(args.export_dir, filename)
    LOGGER.info('Exporting into {}'.format(path))
    
    with gzip.open(path, 'wt') as gzfile:
        writer = csv.writer(gzfile,
                            delimiter=',',
                            quotechar='"')

        while row:
            writer.writerow(row)
            rows_saved += 1
            
            if rows_saved % 20000 == 0:
                LOGGER.info('Exported {} rows from {}.{} table'.format(rows_saved, database_name, table_name))

            row = cursor.fetchone()
    
    # Upload to S3
    LOGGER.info('Uploading to S3...')
    snowflake_config = args.snowflake_config
    client = boto3.client('s3', aws_access_key_id=snowflake_config['aws_access_key_id'], aws_secret_access_key=snowflake_config['aws_secret_access_key'])
    client.upload_file(path, snowflake_config['s3_bucket'], snowflake_config['s3_key_prefix'] + "/" + filename)

    # Delete uploaded files
    LOGGER.info('Deleting uploaded file...')
    os.remove(path)
