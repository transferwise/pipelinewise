"""Process and polling helpers for E2E tests that interrupt real PipelineWise pipelines."""

import json
import subprocess
import time

from pathlib import Path

import psutil


def wait_for(description, predicate, timeout=30, interval=0.1):
    """Poll a predicate until it returns a truthy observable."""
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
        except (FileNotFoundError, json.JSONDecodeError, psutil.Error) as exc:
            last_error = exc
        else:
            if result:
                return result
        time.sleep(interval)

    error_detail = f'; last transient error: {last_error}' if last_error else ''
    raise AssertionError(f'Timed out waiting for {description}{error_detail}')


def run_success(command, env, timeout=120):
    """Run a PipelineWise command and include complete output on failure."""
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        stdout, stderr = stop_process(process)
        raise AssertionError(
            f'Command timed out: {command}\nstdout:\n{stdout}\nstderr:\n{stderr}'
        ) from exc

    assert process.returncode == 0, (
        f'Command failed with return code {process.returncode}: {command}\n'
        f'stdout:\n{stdout}\nstderr:\n{stderr}'
    )
    return stdout


def stop_process(process, timeout=40):
    """Terminate a PipelineWise supervisor, force-killing only its surviving tree."""
    if process.poll() is not None:
        return process.communicate()

    try:
        descendants = psutil.Process(process.pid).children(recursive=True)
    except psutil.Error:
        descendants = []

    process.terminate()
    try:
        return process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        for child in reversed(descendants):
            try:
                child.kill()
            except psutil.Error:
                pass
        process.kill()
        return process.communicate(timeout=10)


def terminate_surviving_processes(processes):
    """Reap only previously captured connector processes left after a failure."""
    alive = []
    for process in processes:
        try:
            if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                process.terminate()
                alive.append(process)
        except psutil.Error:
            continue

    _, alive = psutil.wait_procs(alive, timeout=5)
    for process in alive:
        try:
            process.kill()
        except psutil.Error:
            pass
    psutil.wait_procs(alive, timeout=5)


def read_state(state_path):
    """Read one complete PipelineWise state file."""
    with state_path.open(encoding='utf-8') as state_file:
        return json.load(state_file)


def find_descendant(supervisor, command_name):
    """Find an exact connector command below the PipelineWise supervisor."""
    root = psutil.Process(supervisor.pid)
    for child in root.children(recursive=True):
        try:
            if any(Path(argument).name == command_name for argument in child.cmdline()):
                return child
        except psutil.Error:
            continue
    return None


def find_running_log(log_dir):
    """Return the one active Singer log for this isolated tap."""
    logs = list(log_dir.glob('*.singer.log.running'))
    return logs[0] if len(logs) == 1 else None


def buffered_bytes(mbuffer, minimum_bytes):
    """Measure bytes read from the tap but not written toward the target."""
    current_io = mbuffer.io_counters()
    backlog = current_io.read_chars - current_io.write_chars
    return backlog if backlog >= minimum_bytes else None


def processes_stopped(processes):
    """Return true when every captured child is gone or reaped as a zombie."""
    for process in processes:
        try:
            if process.status() != psutil.STATUS_ZOMBIE:
                return False
        except psutil.NoSuchProcess:
            continue
    return True


def run_cleanups(test_error, cleanups):
    """Attempt every cleanup, then surface failures without masking the test's own error."""
    cleanup_errors = []
    for description, operation in cleanups:
        try:
            operation()
        except Exception as exc:  # Cleanup continues so every resource is attempted.
            cleanup_errors.append((description, exc))

    if cleanup_errors:
        details = '; '.join(
            f'{description}: {error!r}'
            for description, error in cleanup_errors
        )
        cleanup_failure = AssertionError(f'Test cleanup failed: {details}')
        if test_error is None:
            raise cleanup_failure from cleanup_errors[0][1]
        test_error.add_note(str(cleanup_failure))
