"""
PipelineWise CLI - Backend Database to File
"""
import logging
import os
import sys

from .. import utils


class FileDriver:
    """Backend Database Driver - File"""

    def __init__(self):
        """
        Class Constructor

        """
        self.logger = logging.getLogger('Pipelinewise CLI')
        self.config_dir = config_dir
        self.config_path = os.path.join(self.config_dir, 'status.json')

        self.targets = []

    def get_status(self):
        return []

    def save_status(self):
        return

    def get_config(self):
        return []

    def get_object(self):
        return None