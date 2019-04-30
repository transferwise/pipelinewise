import dateutil
import pytz

from tap_s3_csv.logger import LOGGER as logger


def convert_row(row, schema):
    to_return = {}

    for key, value in row.items():
        field_schema = schema['properties'][key]
        datatype = field_schema.get('_conversion_type', 'string')

        logger.debug('Converting {} value {} to {}'.format(
            key, value, datatype))
        converted, _ = convert(value, datatype)

        to_return[key] = converted

    return to_return


def convert(datum, override_type=None):
    """
    Returns tuple of (converted_data_point, json_schema_type,).
    """
    if datum is None or datum == '':
        return (None, None,)

    if override_type in (None, 'integer'):
        try:
            to_return = int(datum)
            return (to_return, 'integer',)
        except (ValueError, TypeError):
            pass

    if override_type in (None, 'number'):
        try:
            to_return = float(datum)
            return (to_return, 'number',)
        except (ValueError, TypeError):
            pass

    if override_type == 'date-time':
        try:
            to_return = dateutil.parser.parse(datum)

            if(to_return.tzinfo is None or
               to_return.tzinfo.utcoffset(to_return) is None):
                to_return = to_return.replace(tzinfo=pytz.utc)

            return (to_return.isoformat(), 'date-time',)
        except (ValueError, TypeError):
            pass

    return (str(datum), 'string',)


def count_sample(sample, start=None):
    if start is None:
        start = {}

    for key, value in sample.items():
        if key not in start:
            start[key] = {}

        (_, datatype) = convert(value)

        if datatype is not None:
            start[key][datatype] = start[key].get(datatype, 0) + 1

    return start


def count_samples(samples):
    to_return = None

    for sample in samples:
        to_return = count_sample(sample, to_return)

    return to_return


def pick_datatype(counts):
    """
    If the underlying records are ONLY of type `integer`, `number`,
    or `date-time`, then return that datatype.

    If the underlying records are of type `integer` and `number` only,
    return `number`.

    Otherwise return `string`.
    """
    to_return = 'string'

    if len(counts) == 1:
        if counts.get('integer', 0) > 0:
            to_return = 'integer'
        elif counts.get('number', 0) > 0:
            to_return = 'number'

    elif(len(counts) == 2 and
         counts.get('integer', 0) > 0 and
         counts.get('number', 0) > 0):
        to_return = 'number'

    return to_return


def generate_schema(samples):
    to_return = {}
    counts = count_samples(samples)

    for key, value in counts.items():
        datatype = pick_datatype(value)

        if datatype == 'date-time':
            to_return[key] = {
                'type': ['null', 'string'],
                'format': 'date-time',
                '_conversion_type': 'date-time',
            }
        else:
            to_return[key] = {
                'type': ['null', datatype],
                '_conversion_type': datatype,
            }

    return to_return
