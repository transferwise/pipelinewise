import enum

from sqlalchemy import Boolean, Column, Enum, Integer, String, Sequence
from sqlalchemy.types import DateTime
from pipelinewise.db.models.base import Base


class TapScheduleState(enum.Enum):
    """Enum class for Schedule states"""
    QUEUING = 'QUEUING'
    READY = 'READY'
    RUNNING = 'RUNNING'
    FAILED = 'FAILED'
    SUCCESS = 'SUCCESS'


class TapSchedule(Base):
    __tablename__ = 'tap_schedule'

    id = Column(
        Integer,
        Sequence('tap_schedule_id_seq'),
        primary_key=True,
        autoincrement=True,
    )
    tap_id = Column(String(255), nullable=False)
    target_id = Column(String(255), nullable=False)
    tap_type = Column(String(255), nullable=False)
    ppw_host = Column(String(255), nullable=False)
    is_enabled = Column(Boolean)
    state = Column(Enum(TapScheduleState), nullable=False)
    sync_period = Column(String(100), nullable=False)
    first_run = Column(DateTime(timezone=True), nullable=False)
    last_run = Column(DateTime(timezone=True), nullable=False)
    adhoc_execute = Column(Boolean)
    adhoc_parameters = Column(String(255))
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))

    def __repr__(self):
        return "<TapSchedule(tap_id='%s', target_id='%s', sync_period='%s', state='%s', is_enabled='%s')>" % (
            self.tap_id,
            self.target_id,
            self.sync_period,
            self.state,
            self.is_enabled,
        )
