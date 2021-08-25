from enum import unique, Enum
from typing import List, Dict, Optional


@unique
class TransformationType(Enum):
    """
    List of supported transformation types
    """
    SET_NULL = 'SET-NULL'
    MASK_HIDDEN = 'MASK-HIDDEN'
    MASK_DATE = 'MASK-DATE'
    MASK_NUMBER = 'MASK-NUMBER'
    HASH = 'HASH'
    HASH_SKIP_FIRST_1 = 'HASH-SKIP-FIRST-1'
    HASH_SKIP_FIRST_2 = 'HASH-SKIP-FIRST-2'
    HASH_SKIP_FIRST_3 = 'HASH-SKIP-FIRST-3'
    HASH_SKIP_FIRST_4 = 'HASH-SKIP-FIRST-4'
    HASH_SKIP_FIRST_5 = 'HASH-SKIP-FIRST-5'
    HASH_SKIP_FIRST_6 = 'HASH-SKIP-FIRST-6'
    HASH_SKIP_FIRST_7 = 'HASH-SKIP-FIRST-7'
    HASH_SKIP_FIRST_8 = 'HASH-SKIP-FIRST-8'
    HASH_SKIP_FIRST_9 = 'HASH-SKIP-FIRST-9'


@unique
class SQLFlavor(Enum):
    """
    List of supported sql flavors
    """
    SNOWFLAKE = 'snowflake'
    POSTGRES = 'postgres'
    BIGQUERY = 'bigquery'


# pylint: disable=too-few-public-methods
class TransformationHelper:
    """
    A helper class for transformations in FastSync
    """

    @classmethod
    def get_trans_in_sql_flavor(
            cls,
            stream_name: str,
            transformations: List[Dict],
            sql_flavor: SQLFlavor) -> List[Dict]:

        """
        Find the transformations to apply to the given stream and does proper formatting and mapping

        Args:
            sql_flavor: sql flavor to use when converting the transformations into sql
            stream_name: the full stream name in the format {schema}-{table}
            transformations: List of transformations

        Returns: list of dictionaries in the form
                {
                    "trans": "...",
                    "conditions": "... AND ... AND ..."
                }
        """

        trans_map = []

        for trans_item in transformations:

            if trans_item.get('tap_stream_name').lower() == stream_name.lower():

                transform_type = TransformationType(trans_item['type'])

                # Make the field id safe in case it's a reserved word
                column = cls.__safe_column(trans_item['field_id'], sql_flavor)

                transform_conditions = trans_item.get('when')

                # get the conditions in "when" and convert them to their SF sql equivalent
                conditions = cls.__conditions_to_sql(transform_conditions, sql_flavor)

                if transform_type == TransformationType.SET_NULL:
                    trans_map.append({
                        'trans': f'{column} = NULL',
                        'conditions': conditions
                    })

                elif transform_type == TransformationType.HASH:

                    trans_map.append({
                        'trans': cls.__hash_to_sql(column, sql_flavor),
                        'conditions': conditions
                    })

                elif transform_type.value.startswith('HASH-SKIP-FIRST-'):

                    trans_map.append({
                        'trans': cls.__hash_skip_first_to_sql(transform_type, column, sql_flavor),
                        'conditions': conditions
                    })

                elif transform_type == TransformationType.MASK_DATE:

                    trans_map.append({
                        'trans': cls.__mask_date_to_sql(column, sql_flavor),
                        'conditions': conditions
                    })

                elif transform_type == TransformationType.MASK_NUMBER:
                    trans_map.append({
                        'trans': f'{column} = 0',
                        'conditions': conditions
                    })

                elif transform_type == TransformationType.MASK_HIDDEN:
                    trans_map.append({
                        'trans': f"{column} = 'hidden'",
                        'conditions': conditions
                    })

        return trans_map

    @classmethod
    # pylint: disable=W0238  # False positive when it is used by another classmethod
    def __conditions_to_sql(
            cls,
            transform_conditions: List[Dict],
            sql_flavor: SQLFlavor) -> Optional[str]:
        """
        Convert the conditional transformations into equivalent form in SF SQL.
        Args:
            transform_conditions: A list of dictionaries of the either form
                {
                    "column": "...",        # required
                    "safe_column": "...",   # optional
                    "equals": <>
                }

                {
                    "column": "...",        # required
                    "safe_column": "...",   # optional
                    "regex_match": "..."
                }

                if no regex_match or equals keys are found, the transformation condition is skipped

        Returns: None if no transformations, otherwise a concatenated string of AND conditions
        """
        if not transform_conditions:
            return None

        conditions = []

        for condition in transform_conditions:
            # for each condition create the sql equivalent of it
            if 'equals' in condition:
                if condition['equals'] is None:
                    operator, value = 'IS', 'NULL'

                elif condition['equals'] == '':
                    operator, value = '=', "''"

                else:
                    operator = '='
                    value = f"'{condition['equals']}'" if isinstance(condition['equals'], str) else condition['equals']

            elif 'regex_match' in condition:

                value = f"'{condition['regex_match']}'"

                if sql_flavor == SQLFlavor.SNOWFLAKE:
                    operator = 'REGEXP'

                elif sql_flavor == SQLFlavor.POSTGRES:
                    operator = '~'

                elif sql_flavor == SQLFlavor.BIGQUERY:
                    conditions.append(f"REGEXP_CONTAINS({cls.__safe_column(condition['column'], sql_flavor)}, {value})")
                    continue

                else:
                    raise NotImplementedError(f'regex_match conditional transformation in {sql_flavor.value} SQL '
                                              f'flavor not implemented!')

            else:
                continue

            conditions.append(f"({cls.__safe_column(condition['column'], sql_flavor)} {operator} {value})")

        return ' AND '.join(conditions)

    @classmethod
    # pylint: disable=W0238  # False positive when it is used by another classmethod
    def __safe_column(cls, col: str, sql_flavor: SQLFlavor):
        # Make the field id safe in case it's a reserved word
        if sql_flavor == SQLFlavor.SNOWFLAKE:
            column = f'"{col.upper()}"'

        elif sql_flavor == SQLFlavor.POSTGRES:
            column = f'"{col.lower()}"'

        elif sql_flavor == SQLFlavor.BIGQUERY:
            column = f'`{col.lower()}`'

        else:
            column = col

        return column

    @classmethod
    # pylint: disable=W0238  # False positive when it is used by another classmethod
    def __hash_to_sql(cls, column: str, sql_flavor: SQLFlavor) -> str:
        """
        convert HASH transformation into the right sql string
        Args:
            column: column to apply the hash to
            sql_flavor: the sql flavor to use

        Raises: NotImplementedError if hash is not implemented for the given sql flavor

        Returns: sql string equivalent of the hash
        """
        if sql_flavor == SQLFlavor.SNOWFLAKE:
            trans = f'{column} = SHA2({column}, 256)'

        elif sql_flavor == SQLFlavor.POSTGRES:
            trans = f'{column} = ENCODE(DIGEST({column}, \'sha256\'), \'hex\')'

        elif sql_flavor == SQLFlavor.BIGQUERY:
            trans = f'{column} = TO_BASE64(SHA256({column}))'

        else:
            raise NotImplementedError(
                f'HASH transformation in {sql_flavor.value} SQL flavor not implemented!')

        return trans

    @classmethod
    # pylint: disable=W0238  # False positive when it is used by another classmethod
    def __hash_skip_first_to_sql(cls, transform_type: TransformationType, column: str, sql_flavor: SQLFlavor) -> str:
        """
        convert HASH-SKIP-FIRST-n transformation into the right sql string
        Args:
            column: column to apply the hash to
            sql_flavor: the sql flavor to use

        Raises: NotImplementedError if hash-skip-first is not implemented for the given sql flavor

        Returns: sql string equivalent of the hash-skip-first
        """

        skip_first_n = transform_type.value[-1]

        if sql_flavor == SQLFlavor.SNOWFLAKE:
            trans = '{0} = CONCAT(SUBSTRING({0}, 1, {1}), SHA2(SUBSTRING({0}, {1} + 1), 256))'.format(
                column, skip_first_n)
        elif sql_flavor == SQLFlavor.POSTGRES:
            trans = '{0} = CONCAT(SUBSTRING({0}, 1, {1}), ENCODE(DIGEST(SUBSTRING({0}, {1} + 1), ' \
                    '\'sha256\'), \'hex\'))'.format(column, skip_first_n)
        elif sql_flavor == SQLFlavor.BIGQUERY:
            trans = '{0} = CONCAT(SUBSTRING({0}, 1, {1}), TO_BASE64(SHA256(SUBSTRING({0}, {1} + 1))))'.format(
                column, skip_first_n)
        else:
            raise NotImplementedError(f'HASH-SKIP-FIRST-{skip_first_n} transformation in {sql_flavor.value} SQL flavor '
                                      f'not implemented!')

        return trans

    @classmethod
    # pylint: disable=W0238  # False positive when it is used by another classmethod
    def __mask_date_to_sql(cls, column: str, sql_flavor: SQLFlavor) -> str:
        """
        convert MASK-DATE transformation into the right sql string
        Args:
            column: column to apply the masking to
            sql_flavor: the sql flavor to use

        Raises: NotImplementedError if hash-skip-first is not implemented for the given sql flavor

        Returns: sql string equivalent of the mask date
        """
        if sql_flavor == SQLFlavor.SNOWFLAKE:
            trans = f'{column} = TIMESTAMP_NTZ_FROM_PARTS(' \
                    f'DATE_FROM_PARTS(YEAR({column}), 1, 1),' \
                    f'TO_TIME({column}))'

        elif sql_flavor == SQLFlavor.POSTGRES:
            trans = '{0} = MAKE_TIMESTAMP(' \
                    'DATE_PART(\'year\', {0})::int, ' \
                    '1, ' \
                    '1, ' \
                    'DATE_PART(\'hour\', {0})::int, ' \
                    'DATE_PART(\'minute\', {0})::int, ' \
                    'DATE_PART(\'second\', {0})::double precision)'.format(column)
        elif sql_flavor == SQLFlavor.BIGQUERY:
            trans = f'{column} = TIMESTAMP(DATETIME(' \
                    f'DATE(EXTRACT(YEAR FROM {column}), 1, 1),' \
                    f'TIME({column})))'
        else:
            raise NotImplementedError(f'MASK-DATE transformation in {sql_flavor.value} SQL flavor '
                                      f'not implemented!')

        return trans
