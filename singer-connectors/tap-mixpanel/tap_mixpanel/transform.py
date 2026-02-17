import datetime

import pytz
import singer
from singer import Transformer
from singer.utils import strftime

LOGGER = singer.get_logger()


def denest_properties(record, properties_node, keep_original_properties=False):
    """De-nest properties for engage and export endpoints. Write fields to
    first level from `properties_node`.

    Args:
        record (dict): Record to update.
        properties_node (str): Nested object whose fields will be written at 1st level.

    Returns:
        dict: Updated record
    """
    new_record = record
    properties = record.get(properties_node)
    if properties:
        for key, val in record[properties_node].items():
            if key[0:1] == "$":
                new_key = f"mp_reserved_{key[1:]}"
                # Change this to regex
            else:
                new_key = key
            new_record[new_key] = val
        if keep_original_properties:
            new_record["properties"] = new_record.pop(properties_node, None)
        else:
            new_record.pop(properties_node, None)
    return new_record


# Reference: https://help.mixpanel.com/hc/en-us/articles/115004547203-Manage-Timezones-for-Projects-in-Mixpanel#exporting-data-from-mixpanel
def transform_event_times(record, project_timezone):
    """Time conversion from $time integer using project_timezone.

    Args:
        record (dict): Record to be transform.
        project_timezone (str): Time zone in which integer date times are stored.

    Returns:
        dict: Updated record.
    """
    new_record = record
    timezone = pytz.timezone(project_timezone)

    # Create beginning_datetime: beginning of epoch time in project timezone
    naive_time = datetime.time(0, 0)
    date = datetime.date(1970, 1, 1)
    naive_datetime = datetime.datetime.combine(date, naive_time)
    # Move 1970-01-01T00:00:00Z to Mixpanel project timezone.
    # For example, if Mixpanel timezone is in Eastern Time (UTC-5:00) then it calculates 1969-12-31T19:00:00-5:00.
    beginning_datetime = pytz.utc.localize(naive_datetime).astimezone(timezone)

    # Get integer time
    time_int = int(record.get("time"))

    # Create new_time_utc by adding seconds to beginning_datetime, normalizing,
    #   and converting to string
    add_seconds = datetime.timedelta(seconds=time_int)
    new_time = beginning_datetime + add_seconds

    # 'normalize' accounts for daylight savings time
    new_time_utc_str = strftime(timezone.normalize(new_time).astimezone(pytz.utc))
    new_record["time"] = new_time_utc_str

    return new_record


def transform_datetime(this_dttm):
    """Transform date_time string TO DATETIME object.

    Args:
        this_dttm (str): Formatted date-time string

    Returns:
        datetime: Datetime object passed string.
    """
    with Transformer() as transformer:
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


def transform_engage(record):
    """Remove leading $ from engage $distinct_id.

    Args:
        record (dict): record to be update.

    Returns:
        dict: New updated record.
    """
    new_record = record
    distinct_id = record.get("$distinct_id")
    new_record["distinct_id"] = distinct_id
    new_record.pop("$distinct_id", None)
    return new_record


def transform_funnels(record, parent_record):
    """Funnels: Combine parent record with each date record

    Args:
        record (dict): Record to be transform.
        parent_record (dict): Parent record.

    Returns:
        dict: Updated record.
    """
    record.update(parent_record)
    return record


def transform_cohort_members(record, parent_record):
    """Cohort Members: provide all distinct_id's for each cohort_id.

    Args:
        record (dict): Record to be transform.
        parent_record (dict): Parent stream  record.

    Returns:
        dict: Record with id fields.
    """
    cohort_id = parent_record.get("id")
    distinct_id = record.get("$distinct_id")
    new_record = {}
    new_record["distinct_id"] = distinct_id
    new_record["cohort_id"] = cohort_id
    return new_record


# Run other transforms, as needed: denest_list_nodes, transform_conversation_parts
def transform_record(
    record,
    stream_name,
    project_timezone,
    parent_record=None,
    denest_properties_flag="true",
):
    """Transform record and add fields at first level as required by stream.

    Args:
        record (dict): Record to be transform.
        stream_name (str): Stream that record belongs to.
        project_timezone (str): Time zone in which integer date times are stored.
        parent_record (dict, optional): Parent stream record if current stream is child.
                                        Defaults to None.

    Returns:
        dict: Transformed record.
    """
    keep_original_properties = str(denest_properties_flag).lower() != "true"
    if stream_name == "engage":
        trans_json = transform_engage(record)
        new_record = denest_properties(
            trans_json,
            "$properties",
            keep_original_properties=keep_original_properties,
        )
    elif stream_name == "export":
        denested_json = denest_properties(
            record,
            "properties",
            keep_original_properties=keep_original_properties,
        )
        new_record = transform_event_times(denested_json, project_timezone)
    elif stream_name == "funnels":
        new_record = transform_funnels(record, parent_record)
    elif stream_name == "cohort_members":
        new_record = transform_cohort_members(record, parent_record)
    else:
        new_record = record

    return new_record
