import multiprocessing
import traceback


class Process(multiprocessing.Process):
    """
    This is an extension of Process to let catching raised exceptions inside the
    process.
    """
    def __init__(self, *args, **kwargs):
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception = None
        self.result_received = False

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._cconn.send(None)
        except Exception as exp:
            t_b = traceback.format_exc()
            self._cconn.send((exp, t_b))

    @property
    def exception(self):
        """
        Returning exception of the process
        """
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception

    def join_with_result(self, poll_seconds=1.0):
        """Join after reading the child's result, so a large exception cannot fill the pipe."""
        # Only the child may hold its end open, so a child that dies mid-send reaches EOF.
        self._cconn.close()
        self.result_received = False
        try:
            # The child blocks in send() until its result is read, so receiving must come first.
            while not self._pconn.poll(poll_seconds):
                if not self.is_alive() and not self._pconn.poll():
                    break
            else:
                self._exception = self._pconn.recv()
                self.result_received = True
        except (EOFError, OSError):
            pass  # a child that died before or while sending has no result
        self.join()
        return self._exception
