"""
PipelineWise CLI - Commands
"""
import os
import logging
from collections import namedtuple

from . import utils
from .errors import StreamBufferTooLargeException

LOGGER = logging.getLogger(__name__)
DEFAULT_STREAM_BUFFER_SIZE = 0          # Disabled by default
DEFAULT_STREAM_BUFFER_BIN = 'mbuffer'
MIN_STREAM_BUFFER_SIZE = 10
MAX_STREAM_BUFFER_SIZE = 1000


TapParams = namedtuple('TapParams', ['type', 'bin', 'config', 'properties', 'state'])
TargetParams = namedtuple('TargetParams', ['type', 'bin', 'config'])
TransformParams = namedtuple('TransformParams', ['bin', 'config'])


def exists_and_executable(bin_path: str) -> bool:
    """
    Checks if a given file exists and executable.
    It checks if the file exists and executable using the given
    absolute or relative path or via one of the path in the
    PATH environment variable

    Args:
         bin_path: Absolute or relative path
    Returns:
        boolean: True if file exists and executable, otherwise False

    """
    if not os.access(bin_path, os.X_OK):
        try:
            paths = f"{os.environ['PATH']}".split(':')
            (p for p in paths if os.access(f'{p}/{bin_path}', os.X_OK)).__next__()
        except StopIteration:
            return False

    return True


def build_tap_command(tap_type: str, tap_bin: str, config: str, properties: str, state: str = None) -> str:
    """
    Builds a command that starts a singer tap connector with the
    required command line arguments

    Args:
        tap_type: One of tap types defined in tap_properties.py
        tap_bin: path the tap python executable
        config: path to config json file
        properties: path to the properties json file
        state: path to the state json file

    Returns:
        string of command line executable
    """
    # Following the singer spec the catalog JSON file needs to be passed by the --catalog argument
    # However some tap (i.e. tap-mysql and tap-postgres) requires it as --properties
    # This is probably for historical reasons and need to clarify on Singer slack channels
    catalog_argument = utils.get_tap_property_by_tap_type(tap_type, 'tap_catalog_argument')

    state_arg = ''
    if state and os.path.isfile(state):
        state_arg = f'--state {state}'

    tap_command = f'{tap_bin} --config {config} {catalog_argument} {properties} {state_arg}'
    return tap_command


def build_target_command(target_bin: str, config: str) -> str:
    """
    Builds a command that starts a singer target connector with the
    required command line arguments

    Args:
        target_bin: path the target python executable
        config: path to config json file

    Returns:
        string of command line executable
    """
    target_command = f'{target_bin} --config {config}'
    return target_command


def build_transformation_command(transform_bin: str, config: str) -> str:
    """
    Builds a command that starts a singer transformation connector
    with the required command line arguments

    Args:
        transform_bin: path to the transform python executable
        config: path to config json file

    Returns:
        string of command line executable if transformation found,
        None otherwise
    """
    trans_command = None

    # Detect if transformation is needed
    if os.path.isfile(config):
        trans = utils.load_json(config)
        if 'transformations' in trans and len(trans['transformations']) > 0:
            trans_command = f'{transform_bin} --config {config}'

    return trans_command


def build_stream_buffer_command(buffer_size: int = 0, stream_buffer_bin: str = DEFAULT_STREAM_BUFFER_BIN) -> str:
    """
    Builds a command that buffers data between tap and target
    connectors to stream data asynchronously. Buffering streams
    avoids blocking taps to extract new data while targets
    is busy with loading the previous batch

    Args:
        buffer_size: Size of buffer in megabytes
        stream_buffer_bin: binary executable of buffer implementation
                           (Default is mbuffer)

    Returns:
        string of command line executable
    Raises:
        StreamBufferTooLargeException if buffer size is greater than
            MAX_STREAM_BUFFER_SIZE
        StreamBufferBinaryNotFound if stream_buffer_bin binary executable
            not found
    """
    buffer_command = None

    if buffer_size and buffer_size > 0:
        # Buffer size cannot be less than min stream buffer size
        if buffer_size < MIN_STREAM_BUFFER_SIZE:
            buffer_size = MIN_STREAM_BUFFER_SIZE
        elif buffer_size > MAX_STREAM_BUFFER_SIZE:
            raise StreamBufferTooLargeException(buffer_size, MAX_STREAM_BUFFER_SIZE)

        buffer_command = f'{stream_buffer_bin} -m {buffer_size}M'

    return buffer_command


def build_singer_command(tap: TapParams, target: TargetParams, transform: TransformParams,
                         stream_buffer_size: int = 0) -> str:
    """
    Builds a command that starts a full singer command with tap,
    target and optional transformation connectors. The connectors are
    connected by linux pipes, following the singer specification.

    Args:
        tap: NamedTuple with tap properties
        target: NamedTuple with target properties
        transform: NamedTuple with transform properties
        stream_buffer_size: in-memory buffer size between tap and target

    Returns:
        string of command line executable
    """
    tap_command = build_tap_command(tap.type,
                                    tap.bin,
                                    tap.config,
                                    tap.properties,
                                    tap.state)
    target_command = build_target_command(target.bin,
                                          target.config)
    transformation_command = build_transformation_command(transform.bin,
                                                          transform.config)
    stream_buffer_command = build_stream_buffer_command(stream_buffer_size)

    # Generate the final piped command with all the required components
    sub_commands = [tap_command, transformation_command, stream_buffer_command, target_command]
    command = ' | '.join(list(filter(None, sub_commands)))

    return command


# pylint: disable=too-many-arguments
def build_fastsync_command(tap: TapParams, target: TargetParams, transform: TransformParams,
                           venv_dir: str, temp_dir: str, tables: str = None) -> str:
    """
    Builds a command that starts fastsync from a given tap to a
    given target with optional transformations.

    Args:
        tap: NamedTuple with tap properties
        target: NamedTuple with target properties
        transform: NamedTuple with transform properties
        venv_dir:
        temp_dir: Temporary dir to generate export temp files
        tables: List of specific tables to fastsync
                (Default is None, to sync every table)

    Returns:
        string of command line executable
    """
    fastsync_bin = utils.get_fastsync_bin(venv_dir, tap.type, target.type)
    command = ' '.join(list(filter(None, [
        f'{fastsync_bin}',
        f'--tap {tap.config}',
        f'--properties {tap.properties}',
        f'--state {tap.state}',
        f'--target {target.config}',
        f'--temp_dir {temp_dir}',
        f'--transform {transform.config}' if transform.config and os.path.isfile(transform.config) else '',
        f'--tables {tables}' if tables else ''])))

    return command
