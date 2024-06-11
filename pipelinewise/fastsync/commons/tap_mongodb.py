import base64
import csv
import datetime
import gzip
import ujson
import logging
import os
import subprocess
import uuid
import bson
import pytz
import tzlocal
from urllib import parse

from typing import Tuple, Optional, Dict, Callable, Any
from pymongo import MongoClient
from pymongo.database import Database
from singer.utils import strftime as singer_strftime

from . import utils, split_gzip
from .errors import (
    ExportError,
    TableNotFoundError,
    MongoDBInvalidDatetimeError,
    UnsupportedKeyTypeException,
)

LOGGER = logging.getLogger(__name__)
DEFAULT_WRITE_BATCH_ROWS = 50000


def serialize_document(document: Dict) -> Dict:
    """
    serialize mongodb Document into a json object

    Args:
        document: MongoDB document

    Returns: Dict
    """
    return {
        key: transform_value(val, [key])
        for key, val in document.items()
        if not isinstance(val, (bson.min_key.MinKey, bson.max_key.MaxKey, bson.binary.Binary))
    }


def class_to_string(key_value: Any, key_type: str) -> str:
    """
    Converts specific types to string equivalent
    The supported types are: datetime, bson Timestamp, bytes, int, Int64, float, ObjectId, str and UUID
    Args:
        key_value: The value to convert to string
        key_type: the value type

    Returns: string equivalent of key value
    Raises: UnsupportedKeyTypeException if key_type is not supported
    """
    if key_type == 'datetime':
        if key_value.tzinfo is None:
            timezone = tzlocal.get_localzone()
            local_datetime = timezone.localize(key_value)
            utc_datetime = local_datetime.astimezone(pytz.UTC)
        else:
            utc_datetime = key_value.astimezone(pytz.UTC)

        return singer_strftime(utc_datetime)

    if key_type == 'Timestamp':
        return '{}.{}'.format(key_value.time, key_value.inc)

    if key_type == 'bytes':
        return base64.b64encode(key_value).decode('utf-8')

    if key_type in ['int', 'Int64', 'float', 'ObjectId', 'str', 'UUID']:
        return str(key_value)

    raise UnsupportedKeyTypeException('{} is not a supported key type'.format(key_type))


def safe_transform_datetime(value: datetime.datetime, path) -> str:
    """
    Safely transform datetime from local tz to UTC if applicable
    Args:
        value: datetime value to transform
        path:

    Returns: utc datetime as string

    """
    timezone = tzlocal.get_localzone()
    try:
        local_datetime = timezone.localize(value)
        utc_datetime = local_datetime.astimezone(pytz.UTC)
    except Exception as ex:
        if str(ex) == 'year is out of range' and value.year == 0:
            # NB: Since datetimes are persisted as strings, it doesn't
            # make sense to blow up on invalid Python datetimes (e.g.,
            # year=0). In this case we're formatting it as a string and
            # passing it along down the pipeline.
            return '{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}.{:06d}Z'.format(
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second,
                value.microsecond,
            )
        raise MongoDBInvalidDatetimeError(
            'Found invalid datetime at [{}]: {}'.format('.'.join(map(str, path)), value)
        ) from ex
    return singer_strftime(utc_datetime)


def transform_value(value: Any, path) -> Any:
    """
    transform values to json friendly ones
    Args:
        value: value to transform
        path:

    Returns: transformed value

    """
    conversion = {
        list: lambda val, pat: list(
            map(lambda v: transform_value(v[1], pat + [v[0]]), enumerate(val))
        ),
        dict: lambda val, pat: {
            k: transform_value(v, pat + [k]) for k, v in val.items()
        },
        uuid.UUID: lambda val, _: class_to_string(val, 'UUID'),
        bson.objectid.ObjectId: lambda val, _: class_to_string(val, 'ObjectId'),
        bson.datetime.datetime: safe_transform_datetime,
        bson.timestamp.Timestamp: lambda val, _: singer_strftime(val.as_datetime()),
        bson.int64.Int64: lambda val, _: class_to_string(val, 'Int64'),
        bytes: lambda val, _: class_to_string(val, 'bytes'),
        datetime.datetime: lambda val, _: class_to_string(val, 'datetime'),
        bson.decimal128.Decimal128: lambda val, _: val.to_decimal(),
        bson.regex.Regex: lambda val, _: dict(pattern=val.pattern, flags=val.flags),
        bson.code.Code: lambda val, _: dict(value=str(val), scope=str(val.scope))
        if val.scope
        else str(val),
        bson.dbref.DBRef: lambda val, _: dict(
            id=str(val.id), collection=val.collection, database=val.database
        ),
    }

    if isinstance(value, tuple(conversion.keys())):
        return conversion[type(value)](value, path)

    return value


def get_connection_string(config: Dict):
    """
    Generates a MongoClientConnectionString based on configuration
    Args:
        config: DB config

    Returns: A MongoClient connection string
    """
    srv = config.get('srv') == 'true'

    # Default SSL verify mode to true, give option to disable
    verify_mode = config.get('verify_mode', 'true') == 'true'
    use_ssl = config.get('ssl') == 'true'

    connection_query = {
        'readPreference': 'secondaryPreferred',
        'authSource': config['auth_database'],
    }

    if config.get('replica_set'):
        connection_query['replicaSet'] = config['replica_set']

    if use_ssl:
        connection_query['ssl'] = 'true'

    # NB: "sslAllowInvalidCertificates" must ONLY be supplied if `SSL` is true.
    if not verify_mode and use_ssl:
        connection_query['tlsAllowInvalidCertificates'] = 'true'

    query_string = parse.urlencode(connection_query)

    connection_string = '{protocol}://{user}:{password}@{host}{port}/{database}?{query_string}'.format(
        protocol='mongodb+srv' if srv else 'mongodb',
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port='' if srv else ':{port}'.format(port=int(config['port'])),
        database=config['database'],
        query_string=query_string
    )

    return connection_string


class FastSyncTapMongoDB:
    """
    Common functions for fastsync from a MongoDB database
    """

    def __init__(self, connection_config: Dict, tap_type_to_target_type: Callable):
        """
        FastSyncTapMongoDB constructor
        Args:
            connection_config: A map of tap source config
            tap_type_to_target_type: Function that maps tap types to target ones
        """
        self.connection_config = connection_config
        self.connection_config['write_batch_rows'] = connection_config.get(
            'write_batch_rows', DEFAULT_WRITE_BATCH_ROWS
        )

        self.connection_config['connection_string'] = get_connection_string(self.connection_config)

        self.tap_type_to_target_type = tap_type_to_target_type
        self.database: Optional[Database] = None

    def open_connection(self):
        """
        Open connection
        """

        self.database = MongoClient(self.connection_config['connection_string'])[
            self.connection_config['database']
        ]

    def close_connection(self):
        """
        Close connection
        """
        self.database.client.close()

    # pylint: disable=R0914,R0913
    def copy_table(
        self,
        table_name: str,
        filepath: str,
        temp_dir: str,
        split_large_files=False,
        split_file_chunk_size_mb=1000,
        split_file_max_chunks=20,
        compress=True,
    ):
        """
        Export data from table to a zipped csv
        Args:
            table_name: Fully qualified table name to export
            filepath: Path where to create the zip file(s) with the exported data
            temp_dir: Temporary directory to export
            split_large_files: Split large files to multiple pieces and create multiple zip files
                               with -partXYZ postfix in the filename. (Default: False)
            split_file_chunk_size_mb: File chunk sizes if `split_large_files` enabled. (Default: 1000)
            split_file_max_chunks: Max number of chunks if `split_large_files` enabled. (Default: 20)
            compress: Flag to indicate whether to compress export files
        """
        table_dict = utils.tablename_to_dict(table_name, '.')

        if table_dict['table_name'] not in self.database.list_collection_names():
            raise TableNotFoundError(f'{table_name} table not found!')

        export_file_path = self._export_collection(temp_dir, table_dict['table_name'])
        extracted_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')

        write_batch_rows = self.connection_config['write_batch_rows']
        exported_rows = 0

        try:
            gzip_splitter = split_gzip.open(
                filepath,
                mode='wt',
                chunk_size_mb=split_file_chunk_size_mb,
                max_chunks=split_file_max_chunks if split_large_files else 0,
                compress=compress,
            )
            with gzip.open(
                export_file_path, 'rb'
            ) as export_file, gzip_splitter as gzfile:
                writer = csv.DictWriter(
                    gzfile,
                    fieldnames=[elem[0] for elem in self._get_collection_columns()],
                    delimiter=',',
                    quotechar='"',
                    quoting=csv.QUOTE_MINIMAL,
                )

                writer.writeheader()
                rows = []

                LOGGER.info('Starting data processing...')

                # bson.decode_file_iter will generate one document at a time from the exported file
                for document in bson.decode_file_iter(export_file):
                    try:
                        rows.append(
                            {
                                '_ID': str(document['_id']),
                                'DOCUMENT': ujson.dumps(serialize_document(document)),
                                utils.SDC_EXTRACTED_AT: extracted_at,
                                utils.SDC_BATCHED_AT: datetime.datetime.utcnow().strftime(
                                    '%Y-%m-%d %H:%M:%S.%f'
                                ),
                                utils.SDC_DELETED_AT: None,
                            }
                        )
                    except TypeError:
                        LOGGER.error(
                            'TypeError encountered when processing document ID: %s',
                            document['_id'],
                        )
                        raise

                    exported_rows += 1

                    # writes batch to csv file and log some nice message on the progress.
                    if exported_rows % write_batch_rows == 0:
                        LOGGER.info(
                            'Exporting batch from %s to %s rows from %s...',
                            (exported_rows - write_batch_rows),
                            exported_rows,
                            table_name,
                        )

                        writer.writerows(rows)
                        rows.clear()

                # write rows one last time
                if rows:
                    LOGGER.info('Exporting last batch ...')
                    writer.writerows(rows)
                    rows.clear()

        finally:
            # whether the code in try succeeds or fails
            # make sure to delete the exported file
            os.remove(export_file_path)

        LOGGER.info('Exported total of %s rows from %s...', exported_rows, table_name)

    @staticmethod
    def _get_collection_columns() -> Tuple:
        """
        Get predefined table/collection column details
        """
        return (
            ('_ID', 'string'),
            ('DOCUMENT', 'object'),
            (utils.SDC_EXTRACTED_AT, 'datetime'),
            (utils.SDC_BATCHED_AT, 'datetime'),
            (utils.SDC_DELETED_AT, 'string'),
        )

    def fetch_current_log_pos(self) -> Dict:
        """
        Find and returns the latest ChangeStream token.
        LOG_BASED method uses changes streams.
        MongoDB doesn't have any built-in feature to get the most recent token atm,
        so a workaround is to start a cursor, grab the first token it returns then exit.

        Returns: token

        """
        token = None

        with self.database.watch(max_await_time_ms=1000) as cursor:
            while cursor.alive:
                _ = cursor.try_next()
                token = cursor.resume_token

                if token is not None:
                    break

        # Token can look like:
        #       {'_data': 'A_LONG_HEX_DECIMAL_STRING'}
        #    or {'_data': 'A_LONG_HEX_DECIMAL_STRING', '_typeBits': b'SOME_HEX'}
        # https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/resume_token.cpp#L82-L96

        # Get the '_data' only from resume token
        # token can contain a property '_typeBits' of type bytes which cannot be json
        # serialized when saving the state in the function 'utils.save_state_file'.
        # '_data' is enough to resume LOG_BASED Singer replication after FastSync
        return {'token': {'_data': token['_data']}}

    # pylint: disable=invalid-name
    def fetch_current_incremental_key_pos(
        self, fully_qualified_table_name: str, replication_key: str
    ):
        """
        No Implemented
        Args:
            fully_qualified_table_name:
            replication_key:
        """
        raise NotImplementedError('INCREMENTAL method is not supported for tap-mongodb')

    def map_column_types_to_target(self):
        """
        Create a map of columns and their target type in addition of primary keys
        Returns: dictionary

        """
        mapped_columns = []

        for column_name, column_type in self._get_collection_columns():
            mapped_columns.append(
                f'{column_name} {self.tap_type_to_target_type(column_type)}'
            )

        return {'columns': mapped_columns, 'primary_key': ['_ID']}

    def _export_collection(self, export_dir: str, collection_name) -> str:
        """
        Dump a collection data into a compressed bson file and returns the path
        Args:
            export_dir: Specifies the directory where dumped file will be
            collection_name: Name of the collection to dump

        Returns: Path to the file

        """
        LOGGER.info('Starting export of table "%s"', collection_name)

        cmd = [
            'mongodump',
            '--uri',
            f'"{self.connection_config["connection_string"]}"',
            '--forceTableScan',
            '--gzip',
            '-c',
            collection_name,
            '-o',
            export_dir,
        ]

        return_code = subprocess.call(cmd)

        LOGGER.debug('Export command return code %s', return_code)

        if return_code != 0:
            raise ExportError(f'Export failed with code {return_code}')

        # mongodump creates two files "{collection_name}.metadata.json.gz" & "{collection_name}.bson.gz"
        # we are only interested in the latter so we delete the former.
        os.remove(
            os.path.join(
                export_dir,
                self.connection_config['database'],
                f'{collection_name}.metadata.json.gz',
            )
        )
        return os.path.join(
            export_dir, self.connection_config['database'], f'{collection_name}.bson.gz'
        )
