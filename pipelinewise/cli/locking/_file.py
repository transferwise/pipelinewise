import os
import time

from ._base import BaseLock


class FileBasedLock(BaseLock):

    def __init__(self, filename='pidfile'):
        self._file = filename
        self._locked = False

    def acquire(self, blocking: bool = True, timeout: int = -1) -> bool:
        if blocking:
            start = time.time()
            while self.locked():
                if timeout > -1 and time.time() - start > timeout:
                    return False
        elif self.locked():
            return False

        with open(self._file, "w") as f:
            f.write(str(os.getpid()))

        return True

    def release(self) -> None:
        if os.path.exists(self._file):
            try:
                os.remove(self._file)
            except OSError:
                pass

    def locked(self) -> bool:
        if os.path.exists(self._file):
            return True
        return False
