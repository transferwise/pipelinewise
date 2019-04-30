import argparse
import json
import singer

import dateutil
import tap_s3_csv.s3 as s3
import tap_s3_csv.conversion as conversion
import tap_s3_csv.config
import tap_s3_csv.format_handler

from tap_s3_csv.logger import LOGGER as logger


def merge_dicts(first, second):
    to_return = first.copy()

    for key in second:
        if key in first:
            if isinstance(first[key], dict) and isinstance(second[key], dict):
                to_return[key] = merge_dicts(first[key], second[key])
            else:
                to_return[key] = second[key]

        else:
            to_return[key] = second[key]

    return to_return


def get_sampled_schema_for_table(config, table_spec):
    logger.info('Sampling records to determine table schema.')

    s3_files = s3.get_input_files_for_table(config, table_spec)

    samples = s3.sample_files(config, table_spec, s3_files)

    metadata_schema = {
        '_s3_source_bucket': {'type': 'string'},
        '_s3_source_file': {'type': 'string'},
        '_s3_source_lineno': {'type': 'integer'},
    }

    data_schema = conversion.generate_schema(samples)

    return {
        'type': 'object',
        'properties': merge_dicts(data_schema, metadata_schema)
    }


def sync_table(config, state, table_spec):
    table_name = table_spec['name']
    modified_since = dateutil.parser.parse(
        state.get(table_name, {}).get('modified_since') or
        config['start_date'])

    logger.info('Syncing table "{}".'.format(table_name))
    logger.info('Getting files modified since {}.'.format(modified_since))

    s3_files = s3.get_input_files_for_table(
        config, table_spec, modified_since)

    logger.info('Found {} files to be synced.'
                .format(len(s3_files)))

    if not s3_files:
        return state

    inferred_schema = get_sampled_schema_for_table(config, table_spec)
    override_schema = {'properties': table_spec.get('schema_overrides', {})}
    schema = merge_dicts(
        inferred_schema,
        override_schema)

    singer.write_schema(
        table_name,
        schema,
        key_properties=table_spec['key_properties'])

    records_streamed = 0

    for s3_file in s3_files:
        records_streamed += sync_table_file(
            config, s3_file['key'], table_spec, schema)

        state[table_name] = {
            'modified_since': s3_file['last_modified'].isoformat()
        }

        singer.write_state(state)

    logger.info('Wrote {} records for table "{}".'
                .format(records_streamed, table_name))

    return state


def sync_table_file(config, s3_file, table_spec, schema):
    logger.info('Syncing file "{}".'.format(s3_file))

    bucket = config['bucket']
    table_name = table_spec['name']

    iterator = tap_s3_csv.format_handler.get_row_iterator(
        config, table_spec, s3_file)

    records_synced = 0

    for row in iterator:
        metadata = {
            '_s3_source_bucket': bucket,
            '_s3_source_file': s3_file,

            # index zero, +1 for header row
            '_s3_source_lineno': records_synced + 2
        }

        to_write = [{**conversion.convert_row(row, schema), **metadata}]
        singer.write_records(table_name, to_write)
        records_synced += 1

    return records_synced


def load_state(filename):
    state = {}

    if filename is None:
        return state

    try:
        with open(filename) as handle:
            state = json.load(handle)
    except:
        logger.fatal("Failed to decode state file. Is it valid json?")
        raise RuntimeError

    return state


# This doesn't originally belong here
"""def do_discover(args):
    import sys
    logger.info("Starting discover")
    streams = None #discover_streams(args)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    logger.info("Finished discover") """


def do_sync(args):
    logger.info('Starting sync.')

    config = tap_s3_csv.config.load(args.config)
    state = load_state(args.state)

    for table in config['tables']:
        state = sync_table(config, state, table)

    logger.info('Done syncing.')



def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')

    args = parser.parse_args()

    try:
        do_sync(args)
    except RuntimeError:
        logger.fatal("Run failed.")
        exit(1)


if __name__ == '__main__':
    main()
