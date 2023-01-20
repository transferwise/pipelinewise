"""
PipelineWise CLI - Alert sender class
"""
# pylint: disable=no-value-for-parameter
import contextlib
from functools import wraps
from inspect import signature
import logging
import os
import datetime

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from croniter import croniter
from sqlalchemy import create_engine
from sqlalchemy import engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import TypeVar

from pipelinewise.db.errors import InvalidBackendDatabaseException
from pipelinewise.db.errors import NotConfiguredBackendDatabaseException
from pipelinewise.db.models.tap_schedule import TapSchedule
from pipelinewise.db.models.tap_schedule import TapScheduleState
from pipelinewise.utils import get_hostname

LOGGER = logging.getLogger(__name__)
RT = TypeVar('RT')  # pylint: disable=invalid-name


class BackendDatabase:
    """
    BackendDatabase class

    Managing operations in the backend database
    """
    def __init__(
        self,
        config: Dict = None,
        auto_upgrade: bool = True
    ) -> None:
        # Initialise backend db as empty dictionary if None provided
        if not config:
            self.config = {}
        else:
            self.config = config
        self.auto_upgrade = auto_upgrade

        # Raise an exception if backend_database is not a dictionary
        if not isinstance(self.config, dict):
            raise InvalidBackendDatabaseException(
                'backend_database config needs to be a dictionary'
            )

        self.enabled = self.config.get('enabled', False)
        self.conn = self.config.get('conn', None)

        if self.enabled and not self.conn:
            raise NotConfiguredBackendDatabaseException(
                'backend metadata database connection is not configured.'
            )

        if self.enabled:
            self.sqlalchemy_url = engine.URL.create(
                drivername=self.conn['driver'],
                host=self.conn['host'],
                port=self.conn['port'],
                username=self.conn['username'],
                password=self.conn['password'],
                database=self.conn['dbname'],
            )
            self.engine = create_engine(str(self.sqlalchemy_url))

            if self.auto_upgrade:
                self.upgrade_db()

    @staticmethod
    def _get_alembic_config():
        current_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = os.path.normpath(os.path.join(current_dir, '..', '..'))
        alembic_dir = os.path.join(root_dir, 'pipelinewise', 'db')
        config = AlembicConfig(os.path.join(root_dir, 'alembic.ini'))
        config.set_main_option('script_location', alembic_dir.replace('%', '%%'))

        return config

    @contextlib.contextmanager
    def _create_session(self):
        """Contextmanager that will create and teardown a session."""
        session = scoped_session(
            sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine,
                expire_on_commit=False,
            )
        )

        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # pylint: disable=no-self-argument,not-callable,protected-access
    def provide_session(func: Callable[..., RT]) -> Callable[..., RT]:
        """
        Function decorator that provides a session if it isn't provided.
        If you want to reuse a session or run the function as part of a
        database transaction, you pass it to the function, if not this wrapper
        will create one and close it for you.
        """
        func_params = signature(func).parameters
        try:
            # func_params is an ordered dict -- this is the "recommended" way of getting the position
            session_args_idx = tuple(func_params).index('session')
        except ValueError:
            raise ValueError(
                f'Function {func.__qualname__} has no `session` argument'
            ) from None
        # We don't need this anymore -- ensure we don't keep a reference to it by mistake
        del func_params

        @wraps(func)
        def wrapper(self, *args, **kwargs) -> RT:
            if 'session' in kwargs or session_args_idx < len(args):
                return func(self, *args, **kwargs)

            with self._create_session() as session:
                return func(self, *args, session=session, **kwargs)

        return wrapper

    def upgrade_db(self):
        """Upgrade backend database schema"""
        LOGGER.info('Upgrading backend database schema...')
        alembic_config = BackendDatabase._get_alembic_config()
        alembic_config.set_main_option('sqlalchemy.url', str(self.sqlalchemy_url))
        alembic_command.upgrade(alembic_config, 'head')

    @staticmethod
    @provide_session
    def create_tap_schedule(session, tap_schedule: TapSchedule):
        """Add a new tap schedule to the backend database"""
        # Set both created_at and updated_at on new tap schedules
        now = datetime.datetime.utcnow()
        tap_schedule.created_at = now
        tap_schedule.updated_at = now

        session.add(tap_schedule)

    @staticmethod
    @provide_session
    def get_tap_schedule(session, tap_id: str, target_id: str) -> Optional[TapSchedule]:
        """Get a tap schedule from the backend database"""
        return session.query(TapSchedule).filter(
            TapSchedule.tap_id == tap_id,
            TapSchedule.target_id == target_id,
        ).one_or_none()

    @staticmethod
    @provide_session
    def update_tap_schedule(
        session,
        tap_schedule_id: int,
        ppw_host: str = None,
        is_enabled: bool = None,
        state: TapScheduleState = None,
        sync_period: str = None,
    ):
        """Update a tap schedule in the backend database"""
        schedule = session.query(TapSchedule).filter(TapSchedule.id == tap_schedule_id).first()

        if schedule:
            # Update the updatable schedule details
            schedule.ppw_host = ppw_host or schedule.ppw_host
            schedule.is_enabled = is_enabled or schedule.is_enabled
            schedule.state = state or schedule.state
            schedule.sync_period = sync_period or schedule.sync_period

            # Update the updated_at timestamp
            schedule.updated_at = datetime.datetime.utcnow()

    @staticmethod
    def refresh_schedules(targets: List[any]):
        """Refresh schedules in the backend database"""
        LOGGER.info('Refreshing schedules in backend database...')
        for target in targets:
            for tap in target['taps']:
                tap_schedule = BackendDatabase.get_tap_schedule(tap_id=tap['id'], target_id=target['id'])

                # Schedule doesn't exist, create it
                if not tap_schedule:
                    BackendDatabase.create_tap_schedule(
                        tap_schedule=TapSchedule(
                            tap_id=tap['id'],
                            target_id=target['id'],
                            tap_type=tap['type'],
                            ppw_host=None,
                            is_enabled=True,
                            state=TapScheduleState.QUEUING,
                            sync_period=tap.get('sync_period'),
                            first_run=datetime.date(1000, 1, 1),
                            last_run=datetime.date(9999, 1, 1),
                            adhoc_execute=False,
                            adhoc_parameters=None,
                        )
                    )

                # Schedule exists, update it
                else:
                    BackendDatabase.update_tap_schedule(
                        tap_schedule_id=tap_schedule.id,
                        # Update only the sync period. All other fields are updated elsewhere.
                        sync_period=tap.get('sync_period'),
                    )

    @staticmethod
    @provide_session
    def attach_taps_to_hosts(session):
        """Find not running taps and attach them to ppw worker hosts"""
        # Plain SQL because SQLAlchemy doesn't support FOR UPDATE SKIP LOCKED
        session.execute(
            """UPDATE tap_schedule
                  SET ppw_host = :ppw_host,
                      state = :ready,
                      updated_at = NOW()
                WHERE id = (
                             SELECT id
                               FROM tap_schedule
                              WHERE is_enabled = TRUE
                                AND state NOT IN (:ready, :running)
                              LIMIT 1 FOR UPDATE SKIP LOCKED
                            )
            """,
            {
                'ppw_host': get_hostname(),
                'ready': TapScheduleState.READY.name,
                'running': TapScheduleState.RUNNING.name,
            }
        )

    @staticmethod
    @provide_session
    def get_taps_to_run(session) -> List[TapSchedule]:
        """Get all taps that are ready to run"""
        taps_to_run = []

        # Get all schedules that are enabled and attached to this host
        tap_schedules = session.query(TapSchedule).filter(
            TapSchedule.is_enabled is True,
            TapSchedule.state == TapScheduleState.READY,
            TapSchedule.first_run <= datetime.datetime.now(),
            TapSchedule.last_run >= datetime.datetime.now(),
            TapSchedule.ppw_host == get_hostname(),
        ).all()

        for tap_schedule in tap_schedules:
            now_minute = datetime.datetime.strptime(
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:00'), '%Y-%m-%d %H:%M:%S')
            iteration = croniter(tap_schedule.sync_period, now_minute - datetime.timedelta(minutes=1))
            next_iter = iteration.get_next(datetime.datetime)

            if now_minute == next_iter:
                BackendDatabase.update_tap_schedule(
                    session=session,
                    tap_schedule_id=tap_schedule.id,
                    state=TapScheduleState.RUNNING
                )
                LOGGER.info('%s -> %s started on ppw_host %s',
                            tap_schedule.tap_id, tap_schedule.target_id, tap_schedule.ppw_host)
                taps_to_run.append(tap_schedule)

        return taps_to_run
