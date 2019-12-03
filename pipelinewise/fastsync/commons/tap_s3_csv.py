import csv
import sys
import gzip
import re
import boto3

from datetime import datetime
from time import struct_time
from typing import Callable, Dict, Set, List, Optional
from singer_encodings import csv as singer_encodings_csv
from singer.utils import strptime_with_tz
from botocore.credentials import DeferredRefreshableCredentials
from messytables import CSVTableSet, headers_guess, headers_processor, offset_processor, type_guess, jts

from .utils import log, safe_column_name, retry_pattern


class FastSyncTapS3Csv:

    def __init__(self, connection_config: Dict, tap_type_to_target_type: Callable):
        """
        Constructor
        :param connection_config: tap connection config
        :param tap_type_to_target_type: callable that maps a tap type to target type
        """
        try:
            # Check if bucket can be accessed without credentials/assuming role
            list(S3Helper.list_files_in_bucket(connection_config['bucket'],
                                               connection_config.get('aws_endpoint_url', None)))
            log("I have direct access to the bucket without assuming the configured role.")
        except:
            # Setup AWS session
            S3Helper.setup_aws_client(connection_config)

        self.connection_config = connection_config
        self.tap_type_to_target_type = tap_type_to_target_type
        self.tables_last_modified = {}

    def _find_table_spec_by_name(self, table_name: str) -> Dict:
        # look in tables array for the full specs dict of given table
        return next(filter(lambda x: x['table_name'] == table_name, self.connection_config['tables']))

    def copy_table(self, table_name: str, file_path: str) -> None:
        """
        Copies data from all csv files that match the search_pattern and into the csv file in file_path
        :param table_name: Name of the table
        :param file_path: Path of the gzip compressed csv file into which data is copied
        :return: None
        """
        if not re.match(r'^.+\.csv\.gz$', file_path):
            raise Exception(f'Invalid file path: {file_path}')

        # find the specs of the table: search_pattern, key_properties ... etc
        table_spec = self._find_table_spec_by_name(table_name)

        # extract the start_date from the specs
        modified_since = strptime_with_tz(self.connection_config['start_date'])

        # get all the files in the bucket that match the criteria and were modified after start date
        s3_files = S3Helper.get_input_files_for_table(self.connection_config, table_spec, modified_since)

        # variable to hold all the records from all matching files
        records = []
        # variable to hold the set of column names from all matching files
        headers = set()

        # given that there might be several files matching the search pattern
        # we want to keep the most recent date one of them was modified to use it as state bookmark
        max_last_modified = None

        for s3_file in s3_files:

            # this function will add records to the `records` list passed to it and add to the `headers` set as well
            self._get_file_records(s3_file['key'], table_spec, records, headers)

            # check if the current file has the most recent modification date
            if max_last_modified is None or max_last_modified < s3_file['last_modified']:
                max_last_modified = s3_file['last_modified']

        # add the found last modified date to the dictionary
        self.tables_last_modified[table_name] = max_last_modified

        # write to the given compressed csv file
        with gzip.open(file_path, 'wt') as gzfile:

            writer = csv.DictWriter(gzfile,
                                    fieldnames=sorted(list(headers)),
                                    # we need to sort the headers so that copying into snowflake works
                                    delimiter=',',
                                    quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            # write the header
            writer.writeheader()
            # write all records at once
            writer.writerows(records)

    def _get_file_records(self, s3_path: str, table_spec: Dict, records: List[Dict], headers: Set) -> None:
        """
        Reads the file in s3_path and inserts the rows in records
        :param config: tap connection configuration
        :param s3_path: full path of file in S3 bucket
        :param table_spec: dict of table with its specs
        :param records: list into which to insert the rows from file
        :param headers: set to update with any new column names
        :return: None
        """
        bucket = self.connection_config['bucket']

        s3_file_handle = S3Helper.get_file_handle(self.connection_config, s3_path)

        # We observed data whose field size exceeded the default maximum of
        # 131072. We believe the primary consequence of the following setting
        # is that a malformed, wide CSV would potentially parse into a single
        # large field rather than giving this error, but we also think the
        # chances of that are very small and at any rate the source data would
        # need to be fixed. The other consequence of this could be larger
        # memory consumption but that's acceptable as well.
        csv.field_size_limit(sys.maxsize)

        iterator = singer_encodings_csv.get_row_iterator(s3_file_handle._raw_stream,
                                                         table_spec)  # pylint:disable=protected-access

        records_copied = len(records)

        for row in iterator:
            now_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
            custom_columns = {
                S3Helper.SDC_SOURCE_BUCKET_COLUMN: bucket,
                S3Helper.SDC_SOURCE_FILE_COLUMN: s3_path,
                S3Helper.SDC_SOURCE_LINENO_COLUMN: records_copied + 1,
                '_SDC_EXTRACTED_AT': now_datetime,
                '_SDC_BATCHED_AT': now_datetime,
                '_SDC_DELETED_AT': None
            }

            new_row = {}

            # make all columns safe
            for k, v in row.items():
                new_row[safe_column_name(k)] = v

            record = {**new_row, **custom_columns}

            records.append(record)
            headers.update(record.keys())

            records_copied += 1

    def map_column_types_to_target(self, filepath: str, table: str):

        csv_columns = self._get_table_columns(filepath)

        specs = None

        for table_o in self.connection_config['tables']:
            if table_o['table_name'] == table:
                specs = table_o
                break

        # use timestamp as a type instead if column is set in date_overrides configuration
        mapped_columns = []
        date_overrides = None if 'date_overrides' not in specs \
            else {[safe_column_name(c) for c in specs['date_overrides']]}

        for column_name, column_type in csv_columns:

            if date_overrides and column_name in date_overrides:
                mapped_columns.append(f"{column_name} 'timestamp_ntz'")
            else:
                mapped_columns.append(f"{column_name} {self.tap_type_to_target_type(column_type)}")

        return {
            "columns": mapped_columns,
            "primary_key": self._get_primary_keys(specs)
        }

    def _get_table_columns(self, csv_file_path: str) -> zip:
        """
        Read the csv file and tries to guess the the type of each column using messytables library.
        The type can be 'Integer', 'Decimal', 'String' or 'Bool'
        :param csv_file_path: path to the csv file with content in it
        :return: a Zip object where each tuple has two elements: the first is the column name and the second is the type
        """
        with gzip.open(csv_file_path, 'rb') as f:
            table_set = CSVTableSet(f)

            row_set = table_set.tables[0]

            offset, headers = headers_guess(row_set.sample)
            row_set.register_processor(headers_processor(headers))

            row_set.register_processor(offset_processor(offset + 1))

            types = [str(t) for t in type_guess(row_set.sample, strict=True)]
            return zip(headers, types)

    def fetch_current_incremental_key_pos(self, table: str,
                                          replication_key: Optional[str] = 'modified_since') -> Optional[Dict]:
        """
        Returns the last time a the table has been modified in ISO format.
        :param table: table name
        :param replication_key: Not needed as it's going to be overrided with `modified_since`
        :return: Dict, e.g {'modified_since': '2019-11-01T07:50:06+00:00'} if the table exists, otherwise None
        """
        replication_key = 'modified_since'

        return {
            replication_key: self.tables_last_modified[table].isoformat()
        } if table in self.tables_last_modified else {}

    def _get_primary_keys(self, table_specs: Dict) -> Optional[str]:
        """
        Returns the primary keys specified in the tap config by key_properties
        The keys are made safe by wrapping them in quotes in case one or more are reserved words.
        :param table_specs: properties of table
        :return: the keys concatenated and separated by comma if keys are given, otherwise None
        """
        if table_specs.get('key_properties', False):
            return ','.join({safe_column_name(k) for k in table_specs['key_properties']})

        return None


# Majority of the code in the class below (S3Helper) is from
# pipelinewise-tap-s3-csv since all AWS S3 operations don't need to change
class S3Helper:
    SDC_SOURCE_BUCKET_COLUMN = "_sdc_source_bucket"
    SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
    SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"

    class AssumeRoleProvider():
        METHOD = 'assume-role'

        def __init__(self, fetcher):
            self._fetcher = fetcher

        def load(self):
            return DeferredRefreshableCredentials(
                self._fetcher.fetch_credentials,
                self.METHOD
            )

    @classmethod
    @retry_pattern()
    def setup_aws_client(cls, config):
        aws_access_key_id = config['aws_access_key_id']
        aws_secret_access_key = config['aws_secret_access_key']

        log("Attempting to create AWS session")
        boto3.setup_default_session(aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key)

    @classmethod
    def get_input_files_for_table(cls, config: Dict, table_spec: Dict, modified_since: struct_time = None):
        bucket = config['bucket']
        prefix = table_spec.get('search_prefix')
        pattern = table_spec['search_pattern']

        try:
            matcher = re.compile(pattern)
        except re.error as e:
            raise ValueError(
                ("search_pattern for table `{}` is not a valid regular "
                 "expression. See "
                 "https://docs.python.org/3.5/library/re.html#regular-expression-syntax").format(
                    table_spec['table_name']),
                pattern) from e

        log(f'Checking bucket "{bucket}" for keys matching "{pattern}"')

        matched_files_count = 0
        unmatched_files_count = 0
        max_files_before_log = 30000

        for s3_object in cls.list_files_in_bucket(bucket, prefix, aws_endpoint_url=config.get('aws_endpoint_url')):
            key = s3_object['Key']
            last_modified = s3_object['LastModified']

            if s3_object['Size'] == 0:
                log(f'Skipping matched file "{key}" as it is empty')
                unmatched_files_count += 1
                continue

            if matcher.search(key):
                matched_files_count += 1
                if modified_since is None or modified_since < last_modified:
                    log(f'Will download key "{key}" as it was last modified {last_modified}')
                    yield {'key': key, 'last_modified': last_modified}
            else:
                unmatched_files_count += 1

            if (unmatched_files_count + matched_files_count) % max_files_before_log == 0:
                # Are we skipping greater than 50% of the files?
                if 0.5 < (unmatched_files_count / (matched_files_count + unmatched_files_count)):
                    log(
                        f"Found {matched_files_count} matching files and {unmatched_files_count} non-matching files. "
                        "You should consider adding a `search_prefix` to the config "
                        "or removing non-matching files from the bucket.")
                else:
                    log(
                        f"Found {matched_files_count} matching files and {unmatched_files_count} non-matching files")

        if 0 == matched_files_count:
            if prefix:
                raise Exception(
                    'No files found in bucket "{}" that matches prefix "{}" and pattern "{}"'.format(bucket, prefix,
                                                                                                     pattern))
            else:
                raise Exception('No files found in bucket "{}" that matches pattern "{}"'.format(bucket, pattern))

    @classmethod
    @retry_pattern()
    def list_files_in_bucket(cls, bucket, search_prefix=None, aws_endpoint_url=None):
        # override default endpoint for non aws s3 services
        if aws_endpoint_url is not None:
            s3_client = boto3.client('s3', endpoint_url=aws_endpoint_url)
        else:
            s3_client = boto3.client('s3')

        s3_object_count = 0

        max_results = 1000
        args = {
            'Bucket': bucket,
            'MaxKeys': max_results,
        }

        if search_prefix is not None:
            args['Prefix'] = search_prefix

        paginator = s3_client.get_paginator('list_objects_v2')
        pages = 0
        for page in paginator.paginate(**args):
            pages += 1
            s3_object_count += len(page.get('Contents', []))
            yield from page.get('Contents', [])

        if 0 < s3_object_count:
            log(f"Found {s3_object_count} files.")
        else:
            log(f'Found no files for bucket "{bucket}" that match prefix "{search_prefix}"')

    @classmethod
    @retry_pattern()
    def get_file_handle(cls, config, s3_path):
        bucket = config['bucket']
        aws_endpoint_url = config.get('aws_endpoint_url')

        # override default endpoint for non aws s3 services
        if aws_endpoint_url is not None:
            s3_client = boto3.resource('s3', endpoint_url=aws_endpoint_url)
        else:
            s3_client = boto3.resource('s3')

        s3_bucket = s3_client.Bucket(bucket)
        s3_object = s3_bucket.Object(s3_path)
        return s3_object.get()['Body']
