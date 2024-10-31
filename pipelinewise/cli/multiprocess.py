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

    def set_start_method_as_spawn(self):
        multiprocessing.set_start_method('spawn', force=True)
