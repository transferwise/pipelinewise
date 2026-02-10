import singer

from tap_mixpanel.streams import STREAMS

LOGGER = singer.get_logger()


def get_streams_to_sync(selected_streams):
    """Get lists of streams to call the sync method.

    For children, ensure that dependent parent_stream is included even
    if it is not selected.
    """
    streams_to_sync = []

    # Loop thru all selected streams
    for stream_name in selected_streams:
        stream_obj = STREAMS[stream_name]
        # If the stream has a parent_stream, then it is a child stream
        parent_stream = hasattr(stream_obj, "parent") and stream_obj.parent

        # Append selected parent streams
        if not parent_stream:
            streams_to_sync.append(stream_name)
        else:
            # Append un-selected parent streams of selected children
            if (
                parent_stream not in selected_streams
                and parent_stream not in streams_to_sync
            ):
                streams_to_sync.append(parent_stream)

    return streams_to_sync


def write_schemas_recursive(stream_id, catalog, selected_streams):
    """Write the schemas for the selected parent and it's all child."""
    # Passing None as client as we are only initializing object to write schema
    stream_obj = STREAMS[stream_id](None)

    if stream_id in selected_streams:
        stream_obj.write_schema(catalog, stream_id)

    # Write schema for selected child
    if stream_obj.child:
        write_schemas_recursive(stream_obj.child, catalog, selected_streams)


def update_currently_syncing(state, stream_name):
    """Currently syncing sets the stream currently being delivered in the
    state.

    If the integration is interrupted, this state property is used to identify
     the starting point to continue from.
    Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
    """
    if (stream_name is None) and ("currently_syncing" in state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client, config, catalog, state, start_date):
    """
    Get selected_streams from catalog, based on state last_stream
    last_stream = Previous currently synced stream, if the load was interrupted
    """
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: %s", last_stream)
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    streams_to_sync = get_streams_to_sync(selected_streams)
    LOGGER.info("selected_streams: %s", selected_streams)
    LOGGER.info("streams_to_sync: %s", streams_to_sync)

    if not selected_streams:
        return

    # Loop through selected_streams
    for stream_name in streams_to_sync:
        stream_obj = STREAMS[stream_name](client)

        update_currently_syncing(state, stream_name)

        # Write schema of only selected streams in parent-child stream
        write_schemas_recursive(stream_name, catalog, selected_streams)

        LOGGER.info("START Syncing: %s", stream_name)
        endpoint_total = stream_obj.sync(
            catalog=catalog,
            state=state,
            config=config,
            start_date=start_date,
            selected_streams=selected_streams,
        )

        update_currently_syncing(state, None)
        LOGGER.info(
            "FINISHED Syncing: %s, Total endpoint records: %s",
            stream_name,
            endpoint_total,
        )
