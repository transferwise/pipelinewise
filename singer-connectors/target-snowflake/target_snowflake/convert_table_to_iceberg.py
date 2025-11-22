import json
import sys
import snowflake.connector
import re
import time

from singer import get_logger

from typing import List, Dict, Union, Tuple, Set
from cryptography.hazmat.primitives import serialization


# pylint: disable=too-many-public-methods,too-many-instance-attributes
class ConvertTableToIceberg:
    """ConvertTableToIceberg class"""

    def __init__(self, connection_config, fqtn=None):
        """
        connection_config:      Snowflake connection details
        fqtn:                   Fully qualified table name to be converted
        """
        # logger to be used across the class's methods
        self.logger = get_logger("convert_table_to_iceberg")
        self.logger.info("Initializing ConvertTableToIceberg for table: %s", self.fqtn)

        self.connection_config = connection_config
        self.fqtn = fqtn

        self.database, self.schema_name, self.table_name = self.parse_fqtn(self.fqtn)

        queries = []
        queries.extend([f"ALTER TABLE {self.fqtn} RENAME TO {self.fqtn}_NATIVE"])
        queries.extend([f"CREATE ICEBERG TABLE {self.fqtn} LIKE {self.fqtn}_NATIVE"])

        self.logger.error(queries)

        # result = self.query(queries)


    def parse_fqtn(self, fqtn: str) -> Tuple[str, str, str]:
        """
        Parse and validate a fully qualified table name.

        Args:
            fqtn: Fully qualified table name in format 'database.schema.table' or '"database"."schema"."table"'

        Returns:
            Tuple of (database, schema, table)

        Raises:
            ValueError: If the FQTN format is invalid
        """
        if not fqtn or not isinstance(fqtn, str):
            raise ValueError("FQTN must be a non-empty string")

        fqtn = fqtn.strip()

        # Pattern to match quoted or unquoted identifiers
        # Matches: database.schema.table or "database"."schema"."table" or mixed
        identifier_pattern = r'(?:"([^"]+)"|([^.]+))'
        full_pattern = rf'^{identifier_pattern}\.{identifier_pattern}\.{identifier_pattern}$'

        match = re.match(full_pattern, fqtn)

        if not match:
            raise ValueError(
                f"Invalid FQTN format: '{fqtn}'. "
                "Expected format: 'database.schema.table' or '\"database\".\"schema\".\"table\"'"
            )

        # Extract matched groups (quoted or unquoted)
        database = match.group(1) or match.group(2)
        schema = match.group(3) or match.group(4)
        table = match.group(5) or match.group(6)

        # Validate that all parts exist
        if not all([database, schema, table]):
            raise ValueError(f"FQTN must contain database, schema, and table: '{fqtn}'")

        # Strip whitespace from each component
        database = database.strip()
        schema = schema.strip()
        table = table.strip()

        # Validate that none are empty after stripping
        if not all([database, schema, table]):
            raise ValueError(f"Database, schema, and table names cannot be empty: '{fqtn}'")

        return database, schema, table


    def open_connection(self):
        """Open snowflake connection"""
        return snowflake.connector.connect(
            user=self.connection_config["user"],
            authenticator="SNOWFLAKE_JWT",
            private_key=self._pem2der(self.connection_config["private_key"]),
            account=self.connection_config["account"],
            database=self.connection_config["dbname"],
            warehouse=self.connection_config["warehouse"],
            role=self.connection_config.get("role", None),
            autocommit=True,
            session_parameters={
                # Quoted identifiers should be case sensitive
                "QUOTED_IDENTIFIERS_IGNORE_CASE": "FALSE",
                "QUERY_TAG": f"convert_table_to_iceberg: {self.fqtn}"
            },
        )

    def _pem2der(self, pem_file: str, password: str = None) -> bytes:
        """Convert Key PEM format to DER format"""
        with open(pem_file, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=password,
            )
        der_key = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return der_key

    def query(self, query: Union[str, List[str]], params: Dict = None, max_records=99999) -> List[Dict]:
        """Run an SQL query in snowflake"""
        result = []

        if params is None:
            params = {}
        else:
            if "LAST_QID" in params:
                self.logger.warning(
                    "LAST_QID is a reserved prepared statement parameter name, "
                    "it will be overridden with each executed query!"
                )

        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:

                # Run every query in one transaction if query is a list of SQL
                if isinstance(query, list):
                    self.logger.debug("Starting Transaction")
                    cur.execute("START TRANSACTION")
                    queries = query
                else:
                    queries = [query]

                qid = None

                # pylint: disable=invalid-name
                for q in queries:

                    # update the LAST_QID
                    params["LAST_QID"] = qid

                    self.logger.debug("Running query: '%s' with Params %s", q, params)

                    cur.execute(q, params)
                    qid = cur.sfqid

                    # Raise exception if returned rows greater than max allowed records
                    if 0 < max_records < cur.rowcount:
                        raise TooManyRecordsException(
                            f"Query returned too many records. This query can return max {max_records} records"
                        )

                    result = cur.fetchall()

        return result

    def create_iceberg_table_query(self, is_temporary=False):
        """Generate CREATE TABLE SQL"""
        stream_schema_message = self.stream_schema_message
        columns = [column_clause(name, schema) for (name, schema) in self.flatten_schema.items()]

        primary_key = []
        if len(stream_schema_message.get("key_properties", [])) > 0:
            pk_list = ", ".join(primary_column_names(stream_schema_message))
            primary_key = [f"PRIMARY KEY({pk_list})"]

        p_temp = "TEMP " if is_temporary else ""
        p_table_name = self.table_name(stream_schema_message["stream"], is_temporary)
        p_columns = ", ".join(columns + primary_key)
        p_extra = "data_retention_time_in_days = 0 " if is_temporary else "data_retention_time_in_days = 1 "
        return f"CREATE {p_temp}TABLE IF NOT EXISTS {p_table_name} ({p_columns}) {p_extra}"

    def get_tables(self, table_schemas=None):
        """Get list of tables of certain schema(s) from snowflake metadata"""
        tables = []
        if table_schemas:
            for schema in table_schemas:
                queries = []

                # Get tables in schema
                show_tables = f"SHOW TERSE TABLES IN SCHEMA {self.connection_config['dbname']}.{schema}"

                # Convert output of SHOW TABLES to table
                select = """
                    SELECT
                        "schema_name" AS schema_name
                        ,"name"       AS table_name
                    FROM TABLE(RESULT_SCAN(%(LAST_QID)s))
                """
                queries.extend([show_tables, select])

                # Run everything in one transaction
                try:
                    tables = self.query(queries, max_records=99999)

                # Catch exception when schema not exists and SHOW TABLES throws a ProgrammingError
                # Regexp to extract snowflake error code and message from the exception message
                # Do nothing if schema not exists
                except snowflake.connector.errors.ProgrammingError as exc:
                    if not re.match(r"002043 \(02000\):.*\n.*does not exist.*", str(sys.exc_info()[1])):
                        raise exc
        else:
            raise Exception("Cannot get table columns. List of table schemas empty")

        return tables

    def iceberg_get_tables(self, table_schemas=None):
        """Get list of iceberg tables of certain schema(s) from snowflake metadata"""
        iceberg_tables = []
        if table_schemas:
            for schema in table_schemas:
                queries = []

                # Get tables in schema
                show_tables = f"SHOW TERSE ICEBERG TABLES IN SCHEMA {self.connection_config['dbname']}.{schema}"

                # Convert output of SHOW ICEBERG TABLES to table
                select = """
                    SELECT
                        "schema_name" AS schema_name
                        ,"name"       AS table_name
                    FROM TABLE(RESULT_SCAN(%(LAST_QID)s))
                """
                queries.extend([show_tables, select])

                # Run everything in one transaction
                try:
                    iceberg_tables.extend(self.query(queries, max_records=99999))

                # Catch exception when schema not exists and SHOW TABLES throws a ProgrammingError
                # Regexp to extract snowflake error code and message from the exception message
                # Do nothing if schema not exists
                except snowflake.connector.errors.ProgrammingError as exc:
                    if not re.match(r"002043 \(02000\):.*\n.*does not exist.*", str(sys.exc_info()[1])):
                        raise exc
        else:
            raise Exception("Cannot get iceberg tables. List of table schemas empty")

        return iceberg_tables

    def get_table_columns(self, table_schemas=None):
        """Get list of columns and tables of certain schema(s) from snowflake metadata"""
        table_columns = []
        if table_schemas:
            for schema in table_schemas:
                queries = []

                # Get column data types by SHOW COLUMNS
                show_columns = f"SHOW COLUMNS IN SCHEMA {self.connection_config['dbname']}.{schema}"

                # Convert output of SHOW COLUMNS to table and insert results into the cache COLUMNS table
                #
                # ----------------------------------------------------------------------------------------
                # Character and numeric columns display their generic data type rather than their defined
                # data type (i.e. TEXT for all character types, FIXED for all fixed-point numeric types,
                # and REAL for all floating-point numeric types).
                # Further info at https://docs.snowflake.net/manuals/sql-reference/sql/show-columns.html
                # ----------------------------------------------------------------------------------------
                select = """
                    SELECT "schema_name" AS schema_name
                          ,"table_name"  AS table_name
                          ,"column_name" AS column_name
                          ,CASE PARSE_JSON("data_type"):type::varchar
                             WHEN 'FIXED' THEN 'NUMBER'
                             WHEN 'REAL'  THEN 'FLOAT'
                             ELSE PARSE_JSON("data_type"):type::varchar
                           END data_type
                      FROM TABLE(RESULT_SCAN(%(LAST_QID)s))
                """

                queries.extend([show_columns, select])

                # Run everything in one transaction
                try:
                    columns = self.query(queries, max_records=99999)

                    if not columns:
                        self.logger.warning(
                            'No columns discovered in the schema "%s"',
                            f"{self.connection_config['dbname']}.{schema}",
                        )
                    else:
                        table_columns.extend(columns)

                # Catch exception when schema not exists and SHOW COLUMNS throws a ProgrammingError
                # Regexp to extract snowflake error code and message from the exception message
                # Do nothing if schema not exists
                except snowflake.connector.errors.ProgrammingError as exc:
                    if not re.match(
                        r"002003 \(02000\):.*\n.*does not exist or not authorized.*",
                        str(sys.exc_info()[1]),
                    ):
                        raise exc

        else:
            raise Exception("Cannot get table columns. List of table schemas empty")

        return table_columns


