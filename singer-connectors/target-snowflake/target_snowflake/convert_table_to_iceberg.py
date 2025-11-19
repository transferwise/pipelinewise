import snowflake.connector
import re

from singer import get_logger

from typing import List, Dict, Union, Tuple, Set
from cryptography.hazmat.primitives import serialization


# pylint: disable=too-many-public-methods,too-many-instance-attributes
class CopyNativeToIceberg:
    """CopyNativeToIceberg class"""

    def __init__(self, connection_config, fqtn=None, eventual='NATIVE'):
        """
        connection_config:      Snowflake connection details
        fqtn:                   Fully qualified table name to be converted
        """
        # logger to be used across the class's methods
        self.logger = get_logger("copy_copy_native_to_iceberg")
        self.logger.info("Initializing CopyNativeToIceberg for table: %s", fqtn)
        self.connection_config = connection_config
        self.fqtn = fqtn
        self.eventual = eventual

        if self.check_iceberg():
            self.logger.info(f'Table {fqtn} already Iceberg')
            exit(1)

        native_columns, iceberg_columns = self.get_columns()
        pk = self.get_pk()

        if eventual == 'NATIVE':
            self.logger.info(f'Creating {fqtn}_ICEBERG and copying data from {fqtn}_NATIVE to {fqtn}_ICEBERG')

        elif eventual == 'ICEBERG':
            self.logger.info(f'Renaming {fqtn} to {fqtn}_NATIVE, Creating ICEBERG table {fqtn} and copying data from {fqtn}_NATIVE to {fqtn}')

            # Rename existing table to _NATIVE
            query=f"ALTER TABLE {fqtn} RENAME TO {fqtn}_NATIVE"
            self.logger.info(query)
            result = self.query(query)
            self.logger.info(result)

        # Create Iceberg table
        query=self.get_create_iceberg(iceberg_columns, pk)
        self.logger.info(query)
        result = self.query(query)
        self.logger.info(result)

        # Copy data to Iceberg
        query=self.get_query_copy_to_iceberg(native_columns)
        self.logger.info(query)
        result = self.query(query)
        self.logger.info(result)


    def check_iceberg(self) -> bool:
        database, schema_name, table_name = self.parse_fqtn(self.fqtn)

        self.logger.info(f'Checking if table {self.fqtn} is an Iceberg table')
        results = self.query(f"SHOW TERSE ICEBERG TABLES LIKE '{table_name}' IN SCHEMA {database}.{schema_name}")
        if len(results) == 0:
            return False
        return True


    def parse_fqtn(self, fqtn: str) -> Tuple[str, str, str]:
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


    def get_columns(self):
        database, schema_name, table_name = self.parse_fqtn(self.fqtn)

        # Query to get column information
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}'
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """

        native_columns = self.query([query])

        iceberg_columns = native_columns.copy()
        # Use Iceberg compatible data types
        for col in iceberg_columns:
            if col['DATA_TYPE'] == 'NUMBER':
                col['DATA_TYPE'] = 'NUMBER(19,0)'
            if col['DATA_TYPE'] == 'TEXT':
                col['DATA_TYPE'] = 'VARCHAR'
            if col['DATA_TYPE'] == 'TIMESTAMP_TZ':
                col['DATA_TYPE'] = 'TIMESTAMP_LTZ'
            if col['DATA_TYPE'] == 'VARIANT':
                col['DATA_TYPE'] = 'VARCHAR'

        return native_columns, iceberg_columns


    def get_pk(self):
        queries = []
        # Query to get primary key constraints
        query = f'SHOW PRIMARY KEYS IN TABLE {self.fqtn};'
        queries.extend([query])

        query = 'select "column_name" as COLUMN_NAME from table(result_scan(-1));'
        queries.extend([query])

        # primary_keys = [row[0] for row in cursor.fetchall()]

        pk = self.query(queries)
        return pk


    def get_create_iceberg(self, columns, pk):
        """Generate CREATE ICEBERG TABLE SQL"""
        database, schema_name, table_name = self.parse_fqtn(self.fqtn)

        if self.eventual == 'NATIVE':
            statement = f"CREATE ICEBERG TABLE {database}.{schema_name}.{table_name}_ICEBERG ( "
        elif self.eventual == 'ICEBERG':
            statement = f"CREATE ICEBERG TABLE {database}.{schema_name}.{table_name} ( "

        # Add column definitions
        column_defs = []
        for col in columns:
            col_name = col['COLUMN_NAME']
            data_type = col['DATA_TYPE']
            column_defs.append(f"{col_name} {data_type}")
        statement += ", ".join(column_defs)

        # Add primary key constraint if exists
        if pk:
            pk_columns = [row['COLUMN_NAME'] for row in pk]
            pk_constraint = f"PRIMARY KEY ({', '.join(pk_columns)})"
            statement += f", {pk_constraint}"
        statement += ")"

        # Add Iceberg table properties
        statement += f" DATA_RETENTION_TIME_IN_DAYS=1"
        statement += f" TARGET_FILE_SIZE='16MB'"
        statement += f" ENABLE_DATA_COMPACTION=TRUE"

        return statement


    def get_query_copy_to_iceberg(self, native_columns):
        database, schema_name, table_name = self.parse_fqtn(self.fqtn)

        # Handle type conversions for native_columns that need casting
        select_columns = []
        for col in native_columns:
            column_name = col['COLUMN_NAME']
            data_type = col['DATA_TYPE']

            # Add casting for native_columns that changed type
            if data_type == 'TIMESTAMP_TZ':
                select_columns.append(f"TO_TIMESTAMP_LTZ({column_name}) AS {column_name}")
            else:
                select_columns.append(f"{column_name}")

        select_clause = ", ".join(select_columns)

        if self.eventual == 'NATIVE':
            statement = f"INSERT INTO {database}.{schema_name}.{table_name}_ICEBERG SELECT {select_clause} FROM {database}.{schema_name}.{table_name}"
        elif self.eventual == 'ICEBERG':
            statement = f"INSERT INTO {database}.{schema_name}.{table_name} SELECT {select_clause} FROM {database}.{schema_name}.{table_name}_NATIVE"

        return statement


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
                "QUERY_TAG": f"copy_native_to_iceberg: {self.fqtn}"
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

    def query(self, query: Union[str, List[str]]) -> List[Dict]:
        """Run an SQL query in snowflake"""
        result = []

        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:

                # Run every query in one transaction if query is a list of SQL
                if isinstance(query, list):
                    self.logger.debug("Starting Transaction")
                    cur.execute("START TRANSACTION")
                    queries = query
                else:
                    queries = [query]

                # pylint: disable=invalid-name
                for q in queries:
                    cur.execute(q)
                    result = cur.fetchall()

        return result
