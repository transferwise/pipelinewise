import os
from datetime import timedelta, datetime

import pytz
import singer
from singer import metadata, utils
from singer.utils import strptime_to_utc

from tap_slack.transform import transform_json

LOGGER = singer.get_logger(__name__)
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
utc = pytz.UTC


class SlackStream:

    def __init__(self, client, config=None, catalog=None, state=None, write_to_singer=True):
        self.client = client
        self.config = config
        self.catalog = catalog
        self.state = state
        self.write_to_singer = write_to_singer
        if config:
            self.date_window_size = int(config.get('date_window_size', '7'))
        else:
            self.date_window_size = 7

    @staticmethod
    def get_abs_path(path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def load_schema(self):
        schema_path = self.get_abs_path('schemas')
        # pylint: disable=no-member
        return singer.utils.load_json('{}/{}.json'.format(schema_path, self.name))

    def write_schema(self):
        schema = self.load_schema()
        # pylint: disable=no-member
        return singer.write_schema(stream_name=self.name, schema=schema,
                                   key_properties=self.key_properties)

    def write_state(self):
        return singer.write_state(self.state)

    def update_bookmarks(self, stream, value):
        if 'bookmarks' not in self.state:
            self.state['bookmarks'] = {}
        self.state['bookmarks'][stream] = value
        LOGGER.info('Stream: {} - Write state, bookmark value: {}'.format(stream, value))
        self.write_state()

    def get_bookmark(self, stream, default):
        # default only populated on initial sync
        if (self.state is None) or ('bookmarks' not in self.state):
            return default
        return self.state.get('bookmarks', {}).get(stream, default)

    def get_absolute_date_range(self, start_date):
        """
        Based on parameters in tap configuration, returns the absolute date range for the sync,
        including the lookback window if applicable.
        :param start_date: The start date in the config, or the last synced date from the bookmark
        :return: the start date and the end date that make up the date range
        """
        lookback_window = self.config.get('lookback_window', '14')
        start_dttm = strptime_to_utc(start_date)
        attribution_window = int(lookback_window)
        now_dttm = utils.now()
        delta_days = (now_dttm - start_dttm).days
        if delta_days < attribution_window:
            start_ddtm = now_dttm - timedelta(days=attribution_window)
        else:
            start_ddtm = start_dttm

        return start_ddtm, now_dttm

    def _all_channels(self):
        types = "public_channel"
        enable_private_channels = self.config.get("private_channels", "false")
        exclude_archived = self.config.get("exclude_archived", "false")
        if enable_private_channels == "true":
            types = "public_channel,private_channel"

        conversations_list = self.client.get_all_channels(types=types,
                                                          exclude_archived=exclude_archived)

        for page in conversations_list:
            channels = page.get('channels')
            for channel in channels:
                yield channel

    def _specified_channels(self):
        for channel_id in self.config.get("channels"):
            yield self.client.get_channel(include_num_members=0, channel=channel_id)

    def channels(self):
        if "channels" in self.config:
            yield from self._specified_channels()
        else:
            yield from self._all_channels()


# ConversationsStream = Slack Channels
class ConversationsStream(SlackStream):
    name = 'channels'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    forced_replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    date_fields = ['created']

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_conversations') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                channels = self.channels()
                for channel in channels:
                    transformed_channel = transform_json(stream=self.name, data=[channel],
                                                         date_fields=self.date_fields)
                    with singer.Transformer(
                            integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                            as transformer:
                        transformed_record = transformer.transform(data=transformed_channel[0], schema=schema,
                                                                   metadata=metadata.to_map(mdata))
                        if self.write_to_singer:
                            singer.write_record(stream_name=self.name,
                                                time_extracted=singer.utils.now(),
                                                record=transformed_record)
                            counter.increment()


# ConversationsMembersStream = Slack Channel Members (Users)
class ConversationMembersStream(SlackStream):
    name = 'channel_members'
    key_properties = ['channel_id', 'user_id']
    replication_method = 'FULL_TABLE'
    forced_replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    date_fields = []

    def sync(self, mdata):

        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_conversation_members') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for channel in self.channels():
                    channel_id = channel.get('id')

                    members_cursor = self.client.get_channel_members(channel_id)

                    for page in members_cursor:
                        members = page.get('members')
                        for member in members:
                            data = {'channel_id': channel_id, 'user_id': member}
                            with singer.Transformer() as transformer:
                                transformed_record = transformer.transform(data=data, schema=schema,
                                                                           metadata=metadata.to_map(
                                                                               mdata))
                                if self.write_to_singer:
                                    singer.write_record(stream_name=self.name,
                                                        time_extracted=singer.utils.now(),
                                                        record=transformed_record)
                                    counter.increment()


# ConversationsHistoryStream = Slack Messages (not including reply threads)
class ConversationHistoryStream(SlackStream):
    name = 'messages'
    key_properties = ['channel_id', 'ts']
    replication_method = 'INCREMENTAL'
    forced_replication_method = 'INCREMENTAL'
    valid_replication_keys = ['channel_id', 'ts']
    date_fields = ['ts']

    # pylint: disable=arguments-differ
    def update_bookmarks(self, channel_id, value):
        """
        For the messages stream, bookmarks are written per-channel.
        :param channel_id: The channel to bookmark
        :param value: The earliest message date in the window.
        :return: None
        """
        if 'bookmarks' not in self.state:
            self.state['bookmarks'] = {}
        if self.name not in self.state['bookmarks']:
            self.state['bookmarks'][self.name] = {}
        self.state['bookmarks'][self.name][channel_id] = value
        self.write_state()

    # pylint: disable=arguments-differ
    def get_bookmark(self, channel_id, default):
        """
        Gets the channel's bookmark value, if present, otherwise a default value passed in.
        :param channel_id: The channel to retrieve the bookmark for.
        :param default: The default value to return if no bookmark
        :return: The bookmark or default value passed in
        """
        # default only populated on initial sync
        if (self.state is None) or ('bookmarks' not in self.state):
            return default
        return self.state.get('bookmarks', {}).get(self.name, {channel_id: default}) \
            .get(channel_id, default)

    # pylint: disable=too-many-branches,too-many-statements
    def sync(self, mdata):

        schema = self.load_schema()
        threads_stream = None
        threads_mdata = None

        # If threads are also being synced we'll need to do that for each message
        for catalog_entry in self.catalog.get_selected_streams(self.state):
            if catalog_entry.stream == 'threads':
                threads_mdata = catalog_entry.metadata
                threads_stream = ThreadsStream(client=self.client, config=self.config,
                                               catalog=self.catalog, state=self.state)

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_conversation_history') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for channel in self.channels():
                    channel_id = channel.get('id')

                    bookmark_date = self.get_bookmark(channel_id, self.config.get('start_date'))
                    start, end = self.get_absolute_date_range(bookmark_date)

                    # Window the requests based on the tap configuration
                    date_window_start = start
                    date_window_end = start + timedelta(days=int(self.date_window_size))
                    min_bookmark = start
                    max_bookmark = start

                    while date_window_start < date_window_end:

                        messages = self.client \
                            .get_messages(channel=channel_id,
                                          oldest=int(date_window_start.timestamp()),
                                          latest=int(date_window_end.timestamp()))

                        if messages:
                            for page in messages:
                                messages = page.get('messages')
                                transformed_messages = transform_json(stream=self.name,
                                                                      data=messages,
                                                                      date_fields=self.date_fields,
                                                                      channel_id=channel_id)
                                for message in transformed_messages:
                                    data = {'channel_id': channel_id}
                                    data = {**data, **message}

                                    # If threads are being synced then the message data for the
                                    # message the threaded replies are in response to will be
                                    # synced to the messages table as well as the threads table
                                    if threads_stream and data.get('thread_ts'):
                                        # If threads is selected we need to sync all the
                                        # threaded replies to this message
                                        threads_stream.write_schema()
                                        threads_stream.sync(mdata=threads_mdata,
                                                            channel_id=channel_id,
                                                            ts=data.get('thread_ts'))
                                        threads_stream.write_state()
                                    with singer.Transformer(
                                            integer_datetime_fmt=
                                            "unix-seconds-integer-datetime-parsing"
                                    ) as transformer:
                                        transformed_record = transformer.transform(
                                            data=data,
                                            schema=schema,
                                            metadata=metadata.to_map(mdata)
                                        )
                                        record_timestamp = data.get('ts', '').partition('.')[0]
                                        record_timestamp_int = int(record_timestamp)
                                        if record_timestamp_int >= start.timestamp():
                                            if self.write_to_singer:
                                                singer.write_record(stream_name=self.name,
                                                                    time_extracted=singer.utils.now(),
                                                                    record=transformed_record)
                                                counter.increment()

                                            if datetime.utcfromtimestamp(
                                                    record_timestamp_int).replace(
                                                tzinfo=utc) > max_bookmark.replace(tzinfo=utc):
                                                # Records are sorted by most recent first, so this
                                                # should only fire once every sync, per channel
                                                max_bookmark = datetime.fromtimestamp(
                                                    record_timestamp_int)
                                            elif datetime.utcfromtimestamp(
                                                    record_timestamp_int).replace(
                                                tzinfo=utc) < min_bookmark:
                                                # The min bookmark tracks how far back we've synced
                                                # during the sync, since the records are ordered
                                                # newest -> oldest
                                                min_bookmark = datetime.fromtimestamp(
                                                    record_timestamp_int)
                                self.update_bookmarks(channel_id,
                                                      max_bookmark.strftime(DATETIME_FORMAT))
                            # Update the date window
                            date_window_start = date_window_end
                            date_window_end = date_window_start + timedelta(
                                days=self.date_window_size)

                            date_window_end = min(date_window_end, end)
                        else:
                            date_window_start = date_window_end


# UsersStream = Slack Users
class UsersStream(SlackStream):
    name = 'users'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'updated'
    valid_replication_keys = ['updated_at']
    date_fields = ['updated']

    def sync(self, mdata):

        schema = self.load_schema()
        bookmark = singer.get_bookmark(state=self.state, tap_stream_id=self.name,
                                       key=self.replication_key)
        if bookmark is None:
            bookmark = self.config.get('start_date')
        new_bookmark = bookmark

        LOGGER.info('Fetching all users that have been updated since %s', bookmark)

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_users') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                # API returns users in no particular order.
                # let's fetch 1000 users per page for now, it saves on api requests and avoids running
                # into rate limiting soon
                users_list = self.client.get_users(limit=1000)

                # this will encounter rate limit at some point
                for page in users_list:
                    users = page.get('members')
                    transformed_users = transform_json(stream=self.name, data=users,
                                                       date_fields=self.date_fields)
                    for user in transformed_users:
                        with singer.Transformer(
                                integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                                as transformer:
                            transformed_record = transformer.transform(data=user, schema=schema,
                                                                       metadata=metadata.to_map(mdata))

                            new_bookmark = max(new_bookmark, transformed_record.get('updated'))
                            if transformed_record.get('updated') > bookmark:
                                if self.write_to_singer:
                                    singer.write_record(stream_name=self.name,
                                                        time_extracted=singer.utils.now(),
                                                        record=transformed_record)
                                    counter.increment()

        LOGGER.info('Updating users state bookmark to %s', new_bookmark)
        self.state = singer.write_bookmark(state=self.state, tap_stream_id=self.name,
                                           key=self.replication_key, val=new_bookmark)


# ThreadsStream = Slack Message Threads (Replies to Slack message)
# The threads stream does a "FULL TABLE" sync using a date window, based on the parent message.
# This means that a thread is only synced if the message it is started on fits within the overall
# sync window. Additionally threaded messages retrieved from the API are only included if they are
# within the overall sync window.
class ThreadsStream(SlackStream):
    name = 'threads'
    key_properties = ['channel_id', 'ts', 'thread_ts']
    replication_method = 'FULL_TABLE'
    replication_key = 'updated'
    valid_replication_keys = ['updated_at']
    date_fields = ['ts', 'last_read']

    def sync(self, mdata, channel_id, ts):
        schema = self.load_schema()
        start, end = self.get_absolute_date_range(self.config.get('start_date'))

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_threads') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                replies = self.client.get_thread(channel=channel_id,
                                                 ts=ts,
                                                 inclusive="true",
                                                 oldest=int(start.timestamp()),
                                                 latest=int(end.timestamp()))

                for page in replies:
                    transformed_threads = transform_json(stream=self.name,
                                                         data=page.get('messages', []),
                                                         date_fields=self.date_fields,
                                                         channel_id=channel_id)
                    for message in transformed_threads:
                        with singer.Transformer(
                                integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                                as transformer:
                            transformed_record = transformer.transform(data=message, schema=schema,
                                                                       metadata=metadata.to_map(
                                                                           mdata))
                            if self.write_to_singer:
                                singer.write_record(stream_name=self.name,
                                                    time_extracted=singer.utils.now(),
                                                    record=transformed_record)
                                counter.increment()


# UserGroupsStream = Slack User Groups
class UserGroupsStream(SlackStream):
    name = 'user_groups'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_user_groups') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                usergroups_list = self.client.get_user_groups(include_count="true",
                                                              include_disabled="true",
                                                              include_user="true")

                for page in usergroups_list:
                    for usergroup in page.get('usergroups'):
                        with singer.Transformer(
                                integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                                as transformer:
                            transformed_record = transformer.transform(data=usergroup,
                                                                       schema=schema,
                                                                       metadata=metadata.to_map(
                                                                           mdata))
                            if self.write_to_singer:
                                singer.write_record(stream_name=self.name,
                                                    time_extracted=singer.utils.now(),
                                                    record=transformed_record)
                                counter.increment()


# TeamsStream = Slack Teams
class TeamsStream(SlackStream):
    name = 'teams'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    replication_key = 'updated'
    valid_replication_keys = ['updated_at']
    date_fields = []

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='team_info') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:

                team_info = self.client.get_teams()

                for page in team_info:
                    team = page.get('team')
                    with singer.Transformer(
                            integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                            as transformer:
                        transformed_record = transformer.transform(data=team,
                                                                   schema=schema,
                                                                   metadata=metadata.to_map(
                                                                       mdata))
                        if self.write_to_singer:
                            singer.write_record(stream_name=self.name,
                                                time_extracted=singer.utils.now(),
                                                record=transformed_record)
                            counter.increment()


# FilesStream = Files uploaded/shared to Slack and hosted by Slack
class FilesStream(SlackStream):
    name = 'files'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'updated'
    valid_replication_keys = ['updated_at']
    date_fields = []

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_files') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:

                bookmark_date = self.get_bookmark(self.name, self.config.get('start_date'))
                start, end = self.get_absolute_date_range(bookmark_date)

                # Window the requests based on the tap configuration
                date_window_start = start
                date_window_end = start + timedelta(days=int(self.date_window_size))
                min_bookmark = start
                max_bookmark = start

                while date_window_start < date_window_end:
                    files_list = self.client.get_files(
                        from_ts=int(date_window_start.timestamp()),
                        to_ts=int(date_window_end.timestamp())
                    )

                    for page in files_list:
                        files = page.get('files')

                        for file in files:
                            with singer.Transformer(
                                    integer_datetime_fmt="unix-seconds-integer-datetime-parsing"
                            ) as transformer:
                                transformed_record = transformer.transform(
                                    data=file,
                                    schema=schema,
                                    metadata=metadata.to_map(mdata)
                                )
                                record_timestamp = \
                                    file.get('timestamp', '')
                                record_timestamp_int = int(record_timestamp)

                                if record_timestamp_int >= start.timestamp():
                                    if self.write_to_singer:
                                        singer.write_record(stream_name=self.name,
                                                            time_extracted=singer.utils.now(),
                                                            record=transformed_record)
                                        counter.increment()

                                    if datetime.utcfromtimestamp(
                                            record_timestamp_int).replace(
                                        tzinfo=utc) > max_bookmark.replace(tzinfo=utc):
                                        # Records are sorted by most recent first, so this
                                        # should only fire once every sync, per channel
                                        max_bookmark = datetime.fromtimestamp(
                                            record_timestamp_int)
                                    elif datetime.utcfromtimestamp(
                                            record_timestamp_int).replace(
                                        tzinfo=utc) < min_bookmark:
                                        # The min bookmark tracks how far back we've synced
                                        # during the sync, since the records are ordered
                                        # newest -> oldest
                                        min_bookmark = datetime.fromtimestamp(
                                            record_timestamp_int)
                        self.update_bookmarks(self.name, max_bookmark.strftime(DATETIME_FORMAT))
                    # Update the date window
                    date_window_start = date_window_end
                    date_window_end = date_window_start + timedelta(
                        days=self.date_window_size)

                    date_window_end = min(date_window_end, end)


# RemoteFilesStream = Files shared to Slack but not hosted by Slack
class RemoteFilesStream(SlackStream):
    name = 'remote_files'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'updated'
    valid_replication_keys = ['updated_at']
    date_fields = []

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_files') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:

                bookmark_date = self.get_bookmark(self.name, self.config.get('start_date'))
                start, end = self.get_absolute_date_range(bookmark_date)

                # Window the requests based on the tap configuration
                date_window_start = start
                date_window_end = start + timedelta(days=int(self.date_window_size))
                min_bookmark = start
                max_bookmark = start

                while date_window_start < date_window_end:
                    remote_files_list = self.client.get_remote_files(
                        from_ts=int(date_window_start.timestamp()),
                        to_ts=int(date_window_end.timestamp())
                    )

                    for page in remote_files_list:
                        remote_files = page.get('files')
                        transformed_files = transform_json(stream=self.name,
                                                           data=remote_files,
                                                           date_fields=self.date_fields)
                        for file in transformed_files:
                            with singer.Transformer(
                                    integer_datetime_fmt="unix-seconds-integer-datetime-parsing"
                            ) as transformer:
                                transformed_record = transformer.transform(
                                    data=file,
                                    schema=schema,
                                    metadata=metadata.to_map(mdata)
                                )
                                record_timestamp = \
                                    file.get('timestamp', '')
                                record_timestamp_int = int(record_timestamp)

                                if record_timestamp_int >= start.timestamp():
                                    if self.write_to_singer:
                                        singer.write_record(stream_name=self.name,
                                                            time_extracted=singer.utils.now(),
                                                            record=transformed_record)
                                        counter.increment()

                                    if datetime.utcfromtimestamp(
                                            record_timestamp_int).replace(
                                        tzinfo=utc) > max_bookmark.replace(tzinfo=utc):
                                        # Records are sorted by most recent first, so this
                                        # should only fire once every sync, per channel
                                        max_bookmark = datetime.fromtimestamp(
                                            record_timestamp_int)
                                    elif datetime.utcfromtimestamp(
                                            record_timestamp_int).replace(
                                        tzinfo=utc) < min_bookmark:
                                        # The min bookmark tracks how far back we've synced
                                        # during the sync, since the records are ordered
                                        # newest -> oldest
                                        min_bookmark = datetime.fromtimestamp(
                                            record_timestamp_int)
                        self.update_bookmarks(self.name, max_bookmark.strftime(DATETIME_FORMAT))
                    # Update the date window
                    date_window_start = date_window_end
                    date_window_end = date_window_start + timedelta(
                        days=self.date_window_size)

                    date_window_end = min(date_window_end, end)


AVAILABLE_STREAMS = {
    "channels": ConversationsStream,
    "users": UsersStream,
    "channel_members": ConversationMembersStream,
    "messages": ConversationHistoryStream,
    "threads": ThreadsStream,
    "user_groups": UserGroupsStream,
    "teams": TeamsStream,
    "files": FilesStream,
    "remote_files": RemoteFilesStream,
}
