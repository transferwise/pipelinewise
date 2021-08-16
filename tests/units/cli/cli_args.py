"""
CLI Arguments Class for unit tests
"""


# pylint: disable=redefined-builtin,too-many-instance-attributes,too-many-arguments
class CliArgs:
    """Class to simulate argparse command line arguments required by PipelineWise class"""

    def __init__(self,
                 target='*',
                 tap='*',
                 tables=None,
                 dir='*',
                 name='*',
                 secret=None,
                 string=None,
                 log='*',
                 extra_log=False,
                 debug=False,
                 profiler=False,
                 enable_fastsync=True,
                 ):
        self.target = target
        self.tap = tap
        self.tables = tables
        self.dir = dir
        self.name = name
        self.secret = secret
        self.string = string
        self.log = log
        self.extra_log = extra_log
        self.enable_fastsync = enable_fastsync
        self.debug = debug
        self.profiler = profiler

    # "log" Getters and setters
    @property
    def log(self):
        """log cli arg"""
        return self.__log

    @log.setter
    def log(self, log):
        self.__log = log

    # "tap" Getters and setters
    @property
    def tap(self):
        """tap cli arg"""
        return self.__tap

    @tap.setter
    def tap(self, tap):
        self.__tap = tap

    # "target" Getters and setters
    @property
    def target(self):
        """target cli arg"""
        return self.__target

    @target.setter
    def target(self, target):
        self.__target = target

    # "dir" Getters and setters
    @property
    def dir(self):
        """dir cli arg"""
        return self.__dir

    @dir.setter
    def dir(self, dir):
        self.__dir = dir

    # "tables" Getters and setters
    @property
    def tables(self):
        """tables cli arg"""
        return self.__tables

    @tables.setter
    def tables(self, tables):
        self.__tables = tables

    # "secret" Getters and setters
    @property
    def secret(self):
        """secret cli arg"""
        return self.__secret

    @secret.setter
    def secret(self, secret):
        self.__secret = secret
