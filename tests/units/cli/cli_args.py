

class CliArgs():
    """Class to simulate argparse command line arguments required by PipelineWise class
    """
    def __init__(self, target='*', tap='*', tables=None, dir='*', name='*', secret=None, string=None, log='*', debug=False):
        self.target = target
        self.tap = tap
        self.tables = tables
        self.dir = dir
        self.name = name
        self.secret = secret
        self.string = string
        self.log = log
        self.debug = debug


    # "log" Getters and setters
    @property
    def log(self):
        return self.__log

    @log.setter
    def log(self, log):
        self.__log = log


    # "tap" Getters and setters
    @property
    def tap(self):
        return self.__tap

    @tap.setter
    def tap(self, tap):
        self.__tap = tap


    # "target" Getters and setters
    @property
    def target(self):
        return self.__target

    @target.setter
    def target(self, target):
        self.__target = target


    # "dir" Getters and setters
    @property
    def dir(self):
        return self.__dir

    @dir.setter
    def dir(self, dir):
        self.__dir = dir


    # "tables" Getters and setters
    @property
    def tables(self):
        return self.__tables

    @tables.setter
    def tables(self, tables):
        self.__tables = tables


    # "secret" Getters and setters
    @property
    def secret(self):
        return self.__secret

    @secret.setter
    def secret(self, secret):
        self.__secret = secret

