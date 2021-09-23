import re
import shlex
import subprocess


def run_command(command):
    """Run shell command and return returncode, stdout and stderr"""
    with subprocess.Popen(
        shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as proc:
        proc_result = proc.communicate()
        return_code = proc.returncode
        stdout = proc_result[0].decode('utf-8')
        stderr = proc_result[1].decode('utf-8')

    return [return_code, stdout, stderr]


def find_run_tap_log_file(stdout, sync_engine=None):
    """Pipelinewise creates log file per running tap instances in a dynamically created directory:
    ~/.pipelinewise/<TARGET_ID>/<TAP_ID>/log

    Every log file matches the pattern:
    <TARGET_ID>-<TAP_ID>-<DATE>_<TIME>.<SYNC_ENGINE>.log.<STATUS>

    The generated full path is logged to STDOUT when tap starting"""
    if sync_engine:
        pattern = re.compile(r'Writing output into (.+\.{}\.log)'.format(sync_engine))
    else:
        pattern = re.compile(r'Writing output into (.+\.log)')

    return pattern.search(stdout).group(1)


def find_profiling_folder(stdout):
    """
    Pipelinewise profiling mode creates a folder where all the stats files are dumped
    This function tries to find that folder from the given stdout output
    Args:
        stdout: output of PPW
    Returns: profiling folder as string
    """
    pattern = re.compile(r'Profiling stats files are in folder "(.+)"')

    return pattern.search(stdout).group(1)
