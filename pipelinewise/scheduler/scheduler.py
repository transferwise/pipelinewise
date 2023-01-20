
import logging
import sched
import subprocess

from pipelinewise.db.backend_database import BackendDatabase
from pipelinewise.db.models.tap_schedule import TapScheduleState
from pipelinewise.scheduler.errors import InvalidSyncPeriodException
from pipelinewise.scheduler.errors import NoSyncPeriodException

LOGGER = logging.getLogger(__name__)


class Scheduler:
    """
    Scheduler class
    """
    def __init__(
        self,
        backend_db: BackendDatabase,
        attach_taps_to_hosts_interval: int = 5,
        run_taps_interval: int = 5,
        check_child_processes_interval: int = 1,
    ) -> None:
        self.backend_db = backend_db
        self.timers = sched.scheduler()
        self.attach_taps_to_hosts_interval = attach_taps_to_hosts_interval
        self.run_taps_interval = run_taps_interval
        self.check_child_processes_interval = check_child_processes_interval
        self.child_processes = {}

    def _check_child_processes(self, argument=()):
        LOGGER.debug(f'Total number of child_processes: {len(self.child_processes)}')
        finished_tap_schedule_ids = []

        for tap_schedule_id, child_process in self.child_processes.items():
            returncode = child_process.poll()

            if returncode is not None:
                LOGGER.info(f'Tap {tap_schedule_id} finished with returncode {returncode}')
                if returncode == 0:
                    next_state = TapScheduleState.SUCCESS
                else:
                    next_state = TapScheduleState.FAILED

                # Update tap schedule state
                self.backend_db.update_tap_schedule(
                    id=tap_schedule_id,
                    ppw_host='',
                    state=next_state,
                )

                # Add to finished items, we can't remove them from
                # the dict while iterating
                finished_tap_schedule_ids.append(tap_schedule_id)

        # Remove finished items from child processes dict
        for key in finished_tap_schedule_ids:
            del self.child_processes[key]

        self.timers.enter(self.check_child_processes_interval, 1, self._check_child_processes, argument=argument)

    def attach_taps_to_hosts(self, argument=()):
        try:
            self.backend_db.attach_taps_to_hosts()
        except (NoSyncPeriodException, InvalidSyncPeriodException) as e:
            LOGGER.error('Error attaching taps to hosts: %s', str(e))

        self.timers.enter(self.attach_taps_to_hosts_interval, 1, self.attach_taps_to_hosts, argument=argument)

    def run_taps(self, argument=()):
        tap_schedules_to_run = self.backend_db.get_taps_to_run()
        for tap_schedule in tap_schedules_to_run:
            try:
                command = [
                    'pipelinewise',
                    'run_tap',
                    '--tap', tap_schedule.tap_id,
                    '--target', tap_schedule.target_id
                ]
                child_process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.child_processes[tap_schedule.id] = child_process
            except Exception as err:
                LOGGER.error(f'Error running {tap_schedule.tap_id}: {err}')
                self.backend_db.update_tap_schedule(
                    id=tap_schedule.id,
                    ppw_host='',
                    state=TapScheduleState.FAILED
                )

        self.timers.enter(self.run_taps_interval, 1, self.run_taps, argument=argument)

    def run(self):
        self.timers.enter(self.attach_taps_to_hosts_interval, 1, self.attach_taps_to_hosts, argument=())
        self.timers.enter(self.run_taps_interval, 1, self.run_taps, argument=())
        self.timers.enter(self.check_child_processes_interval, 1, self._check_child_processes, argument=())
        self.timers.run()
