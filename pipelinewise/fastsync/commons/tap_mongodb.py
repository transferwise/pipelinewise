import base64
import csv
import datetime
import gzip
import json
import logging
import os
import ssl
import subprocess
import uuid
import bson
import pytz
import tzlocal

from typing import Tuple, Optional, Dict, Callable
from pymongo import MongoClient
from pymongo.database import Database
from singer.utils import strftime as singer_strftime

from . import utils
from .errors import ExportError, TableNotFoundError, MongoDBInvalidDatetimeError

LOGGER = logging.getLogger(__name__)
DEFAULT_WRITE_BATCH_ROWS = 50000
BSON_CodecOptions = bson.CodecOptions(
    uuid_representation=3,
    unicode_decode_error_handler='ignore')


class MongoDBJsonEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to be used to serialize data from MongoDB
    """
    @staticmethod
    def _serialize_datetime(val):
        """
        Serialize Bson and python datetime types
        Args:
            val: datetime value

        Returns: serialized datetime value

        """
        if isinstance(val, bson.datetime.datetime):
            timezone = tzlocal.get_localzone()
            try:
                local_datetime = timezone.localize(val)
                utc_datetime = local_datetime.astimezone(pytz.UTC)
            except Exception as ex:
                if str(ex) == 'year is out of range' and val.year == 0:
                    # NB: Since datetimes are persisted as strings, it doesn't
                    # make sense to blow up on invalid Python datetimes (e.g.,
                    # year=0). In this case we're formatting it as a string and
                    # passing it along down the pipeline.
                    return '{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}.{:06d}Z'.format(val.year,
                                                                                      val.month,
                                                                                      val.day,
                                                                                      val.hour,
                                                                                      val.minute,
                                                                                      val.second,
                                                                                      val.microsecond)
                raise MongoDBInvalidDatetimeError('Found invalid datetime {}'.format(val))

            return singer_strftime(utc_datetime)

        if isinstance(val, datetime.datetime):
            timezone = tzlocal.get_localzone()
            local_datetime = timezone.localize(val)
            utc_datetime = local_datetime.astimezone(pytz.UTC)
            return singer_strftime(utc_datetime)
        return None

    def default(self, o): # false positive complaint -> pylint: disable=E0202
        """
        Custom function to serialize several sort of BSON and Python types
        Args:
            obj: Object to serialize

        Returns: Serialized value
        """
        encoding_map = {
            bson.objectid.ObjectId: str,
            uuid.UUID: str,
            bson.int64.Int64: str,
            bson.timestamp.Timestamp: lambda value: singer_strftime(value.as_datetime()),
            bytes: lambda value: base64.b64encode(value).decode('utf-8'),
            bson.decimal128.Decimal128: lambda val: val.to_decimal(),
            bson.regex.Regex: lambda val: dict(pattern=val.pattern, flags=val.flags),
            bson.code.Code: lambda val: dict(value=str(val), scope=str(val.scope)) if val.scope else str(val),
            bson.dbref.DBRef: lambda val: dict(id=str(val.id), collection=val.collection, database=val.database),
            datetime.datetime: self._serialize_datetime,
            bson.datetime.datetime: self._serialize_datetime
        }

        if o.__class__ in encoding_map:
            return encoding_map[o.__class__](o)

        return super(MongoDBJsonEncoder, self).default(o)


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
        self.connection_config['write_batch_rows'] = connection_config.get('write_batch_rows',
                                                                           DEFAULT_WRITE_BATCH_ROWS)

        self.tap_type_to_target_type = tap_type_to_target_type
        self.database: Optional[Database] = None

    def open_connection(self):
        """
        Open connection
        """
        # Default SSL verify mode to true, give option to disable
        verify_mode = self.connection_config.get('verify_mode', 'true') == 'true'
        use_ssl = self.connection_config.get('ssl') == 'true'

        connection_params = dict(host=self.connection_config['host'], port=int(self.connection_config['port']),
                                 username=self.connection_config['user'], password=self.connection_config['password'],
                                 authSource=self.connection_config['auth_database'], ssl=use_ssl,
                                 replicaSet=self.connection_config.get('replica_set', None),
                                 readPreference='secondaryPreferred')

        # NB: "ssl_cert_reqs" must ONLY be supplied if `SSL` is true.
        if not verify_mode and use_ssl:
            connection_params['ssl_cert_reqs'] = ssl.CERT_NONE

        self.database = MongoClient(**connection_params)[self.connection_config['database']]

    def close_connection(self):
        """
        Close connection
        """
        self.database.client.close()

    def copy_table(self, table_name: str, filepath: str, temp_dir: str):
        """
        Export data from table to a zipped csv
        """
        table_dict = utils.tablename_to_dict(table_name, '.')

        if table_dict['table_name'] not in self.database.list_collection_names():
            raise TableNotFoundError(f'{table_name} table not found!')

        export_file_path = self._export_collection(temp_dir, table_dict['table_name'])
        extracted_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')

        write_batch_rows = self.connection_config['write_batch_rows']
        exported_rows = 0

        try:
            with gzip.open(export_file_path, 'rb') as export_file, gzip.open(filepath, 'wt') as gzfile:
                writer = csv.DictWriter(gzfile,
                                        fieldnames=[elem[0] for elem in self._get_collection_columns()],
                                        delimiter=',',
                                        quotechar='"',
                                        quoting=csv.QUOTE_MINIMAL)

                writer.writeheader()
                rows = []

                LOGGER.info('Starting data processing...')

                # bson.decode_file_iter will generate one document at a time from the exported file
                for document in bson.decode_file_iter(export_file, codec_options=BSON_CodecOptions):
                    rows.append({
                        '_ID': str(document['_id']),
                        'DOCUMENT': json.dumps(document, cls=MongoDBJsonEncoder, separators=(',', ':')),
                        utils.SDC_EXTRACTED_AT: extracted_at,
                        utils.SDC_BATCHED_AT: datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'),
                        utils.SDC_DELETED_AT: None
                    })

                    exported_rows += 1

                    # writes batch to csv file and log some nice message on the progress.
                    if exported_rows % write_batch_rows == 0:
                        LOGGER.info(
                            'Exporting batch from %s to %s rows from %s...',
                            (exported_rows - write_batch_rows),
                            exported_rows, table_name
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

    def fetch_current_log_pos(self)->Dict:
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
        return {
            'token': {
                '_data': token['_data']
            }
        }

    # pylint: disable=invalid-name
    def fetch_current_incremental_key_pos(self, fully_qualified_table_name: str, replication_key: str):
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
            mapped_columns.append(f'{column_name} {self.tap_type_to_target_type(column_type)}')

        return {
            'columns': mapped_columns,
            'primary_key': ['_ID']
        }

    def _export_collection(self, export_dir: str, collection_name)->str:
        """
        Dump a collection data into a compressed bson file and returns the path
        Args:
            export_dir: Specifies the directory where dumped file will be
            collection_name: Name of the collection to dump

        Returns: Path to the file

        """
        LOGGER.info('Starting export of table "%s"', collection_name)

        url = f'mongodb://{self.connection_config["user"]}:{self.connection_config["password"]}' \
              f'@{self.connection_config["host"]}:{self.connection_config["port"]}/' \
              f'{self.connection_config["database"]}?authSource={self.connection_config["auth_database"]}' \
              f'&readPreference=secondaryPreferred'

        if self.connection_config.get('replica_set', None) is not None:
            url += f'&replicaSet={self.connection_config["replica_set"]}'

        return_code = subprocess.call([
            'mongodump',
            '--uri', f'"{url}"',
            '--forceTableScan',
            '--gzip',
            '-c', collection_name,
            '-o', export_dir
        ])

        LOGGER.debug('Export command return code %s', return_code)

        if return_code != 0:
            raise ExportError(f'Export failed with code {return_code}')

        #mongodump creates two files "{collection_name}.metadata.json.gz" & "{collection_name}.bson.gz"
        # we are only interested in the latter so we delete the former.
        os.remove(os.path.join(export_dir, self.connection_config['database'], f'{collection_name}.metadata.json.gz'))
        return os.path.join(export_dir, self.connection_config['database'], f'{collection_name}.bson.gz')
