from abc import ABC, abstractmethod
from types import TracebackType
from typing import Optional, Type


class _LockTimeout(Exception):
    pass


class BaseLock(ABC):
    @abstractmethod
    def acquire(self, blocking: bool = True, timeout: int = -1) -> bool:
        pass

    @abstractmethod
    def release(self) -> None:
        pass

    @abstractmethod
    def locked(self) -> bool:
        pass

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType]
    ) -> None:
        return self.release()
