"""
PipelineWise CLI - Status class
"""
import logging
import os
import sys

from . import utils

from .db.file_driver import FileDriver


class Status():
    """PipelineWise Status Class"""

    def __init__(self, abc):
        """
        Class Constructor

        Initialising status with an empty list of data flows
        """
        self.abc = abc
        #print(db_driver)


    def get_abc(self):
        return self.abc
