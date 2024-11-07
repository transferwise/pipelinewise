import hashlib
import re

from typing import Dict, Any, Optional, List
from dpath.util import get as get_xpath, set as set_xpath
from singer import get_logger
from dateutil import parser

LOGGER = get_logger('transform_field')


def is_transform_required(record: Dict, when: Optional[List[Dict]]) -> bool:
    """
        Detects if the transformation is required or not based on
        the defined conditions and the actual values in a record.
        All conditions in when need to be met for the transformation to be required.
    """
    if not when:
        # Transformation is always required if 'when' condition not defined
        LOGGER.debug('No conditions, transformations is required')
        return True

    transform_required = False

    # Check if conditional transformation matches criteria
    # Evaluate every condition
    for condition in when:
        column_to_match = condition['column']
        column_value = record.get(column_to_match, "")

        field_path_to_match = condition.get('field_path')

        # check if given field exists in the column value
        if field_path_to_match:
            try:
                field_value = get_xpath(column_value, field_path_to_match)
                LOGGER.debug('field "%s" exists in the value of column "%s"', field_path_to_match, column_to_match)

            except KeyError:
                # KeyError exception means the field doesn't exist, hence we cannot proceed with the
                # equals/regex match condition, thus the condition isn't met and don't need to do
                # transformation so breaking prematurely
                transform_required = False

                LOGGER.debug('field "%s" doesn\'t exists in the value of column "%s", '
                             'so transformation is not required.', field_path_to_match, column_to_match)
                break

        cond_equals = condition.get('equals')
        cond_pattern = condition.get('regex_match')

        # Exact condition
        if cond_equals:
            LOGGER.debug('Equals condition found, value is: %s', cond_equals)
            if field_path_to_match:
                transform_required = __is_condition_met('equal', cond_equals, field_value)
            else:
                transform_required = __is_condition_met('equal', cond_equals, column_value)

            # Condition isn't met, exit the loop
            if not transform_required:
                LOGGER.debug('Equals condition didn\'t match, so transformation is not required.')
                break

        # Regex based condition
        elif cond_pattern:
            LOGGER.debug('Regex condition found, pattern is: %s', cond_pattern)

            if field_path_to_match:
                transform_required = __is_condition_met('regex', cond_pattern, field_value)
            else:
                transform_required = __is_condition_met('regex', cond_pattern, column_value)

            # Condition isn't met, exit the loop
            if not transform_required:
                LOGGER.debug('Regex pattern didn\'t match, so transformation is not required.')
                break

    LOGGER.debug('Transformation required? %s', transform_required)

    return transform_required


def __is_condition_met(condition_type: str, condition_value: Any, value: Any) -> bool:
    """
    Checks if given value meets the given condition
    Args:
        condition_type: condition type, could be "equal" or "regex"
        condition_value: the value of the condition, in case of regex it's the pattern, and
                         a value to compare to in case of equal
        value: the target value to run the condition against

    Returns: bool, True of condition is met, False otherwise
    """

    if condition_type == 'equal':
        return value == condition_value

    if condition_type == 'regex':
        matcher = re.compile(condition_value)
        return bool(matcher.search(value))

    raise NotImplementedError(f'__is_condition_met is not implemented for condition type "{condition_type}"', )


def do_transform(record: Dict,
                 field: str,
                 trans_type: str,
                 when: Optional[List[Dict]] = None,
                 field_paths: Optional[List[str]] = None
                 ) -> Any:
    """Transform a value by a certain transformation type.
    Optionally can set conditional criteria based on other
    values of the record"""

    return_value = value = record.get(field)

    try:
        # Do transformation only if required
        if is_transform_required(record, when):

            # transforming fields nested in value dictionary
            if isinstance(value, dict) and field_paths:
                for field_path in field_paths:
                    try:
                        field_val = get_xpath(value, field_path)
                        set_xpath(value, field_path, _transform_value(field_val, trans_type))
                    except KeyError:
                        LOGGER.error('Field path %s does not exist', field_path)

                return_value = value

            else:
                return_value = _transform_value(value, trans_type)

        # Return the original value if transformation is not required
        else:
            return_value = value

        return return_value

    # Return the original value if cannot transform
    except Exception:
        return return_value


def _transform_value(value: Any, trans_type: str) -> Any:
    """
    Applies the given transformation type to the given value
    Args:
        value: value to transform
        trans_type: transformation type to apply

    Returns:
        transformed value
    """
    # Transforms any input to NULL
    if trans_type == "SET-NULL":
        return_value = None

    # Transforms string input to hash
    elif trans_type == "HASH":
        return_value = hashlib.sha256(value.encode('utf-8')).hexdigest()

    # Transforms string input to hash skipping first n characters, e.g. HASH-SKIP-FIRST-2
    elif 'HASH-SKIP-FIRST' in trans_type:
        return_value = value[:int(trans_type[-1])] + \
                       hashlib.sha256(value.encode('utf-8')[int(trans_type[-1]):]).hexdigest()

    # Transforms any date to stg
    elif trans_type == "MASK-DATE":
        return_value = parser.parse(value).replace(month=1, day=1).isoformat()

    # Transforms any number to zero
    elif trans_type == "MASK-NUMBER":
        return_value = 0

    # Transforms any value to "hidden"
    elif trans_type == "MASK-HIDDEN":
        return_value = 'hidden'

    # Transforms string input to masked version skipping first and last n characters
    # e.g. MASK-STRING-SKIP-ENDS-3
    elif 'MASK-STRING-SKIP-ENDS' in trans_type:
        skip_ends_n = int(trans_type[-1])
        value_len = len(value)
        return_value = '*' * value_len if value_len <= (2 * skip_ends_n) \
            else f'{value[:skip_ends_n]}{"*" * (value_len - (2 * skip_ends_n))}{value[-skip_ends_n:]}'

    # Return the original value if cannot find transformation type
    # todo: is this the right behavior?
    else:
        LOGGER.warning('Cannot find transformation type %s, returning same value', trans_type)
        return_value = value

    return return_value
