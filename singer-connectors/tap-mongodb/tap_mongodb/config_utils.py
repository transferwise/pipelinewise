from typing import Dict

from tap_mongodb.errors import InvalidAwaitTimeError, InvalidUpdateBufferSizeError
from tap_mongodb.sync_strategies import change_streams


def validate_config(config: Dict) -> None:
    """
    Goes through the config and validate it
    Currently, only few parameters are validated
    Args:
        config: Dictionary of config to validate

    Returns: None
    Raises: InvalidUpdateBufferSizeError or InvalidAwaitTimeError
    """
    if 'update_buffer_size' in config:
        update_buffer_size = config['update_buffer_size']

        if not isinstance(update_buffer_size, int):
            raise InvalidUpdateBufferSizeError(update_buffer_size, 'Not integer')

        if not (change_streams.MIN_UPDATE_BUFFER_LENGTH <=
                update_buffer_size <= change_streams.MAX_UPDATE_BUFFER_LENGTH):

            raise InvalidUpdateBufferSizeError(
                update_buffer_size,
                f'Not in the range [{change_streams.MIN_UPDATE_BUFFER_LENGTH}..'
                f'{change_streams.MAX_UPDATE_BUFFER_LENGTH}]')


    if 'await_time_ms' in config:
        await_time_ms = config['await_time_ms']

        if not isinstance(await_time_ms, int):
            raise InvalidAwaitTimeError(await_time_ms, 'Not integer')

        if await_time_ms <= 0:
            raise InvalidAwaitTimeError(
                await_time_ms, 'time must be > 0')
