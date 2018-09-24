#!/usr/bin/env python3
import os
from pathlib import Path

from pkg_resources import get_distribution
from cli.pipelinewise import PipelineWise
import argparse

__version__ = get_distribution('cli').version
config_dir = os.path.join(Path.home(), '.pipelinewise')
venv_dir = os.path.join(os.getcwd(), '../.virtualenvs')

commands = [
  'run_tap',
  'discover_tap',
  'test_tap_connection'
]

command_help = """Available commands, """ + ','.join(commands)
target_help = """Name of the target"""
tap_help = """Name of the tap"""
version_help = """Displays the installed versions"""
debug_help = """Forces the debug mode with logging on stdout and log level debug."""

def main():
    '''Main entry point'''
    parser = argparse.ArgumentParser(description='TransferData {} - Command Line Interface'.format(__version__), add_help=True)
    parser.add_argument('command', type=str, help=command_help)
    parser.add_argument('--target', type=str, required=True, help=target_help)
    parser.add_argument('--tap', type=str, required=True, help=tap_help)
    parser.add_argument('--version', action="version", help=version_help, version='TransferData {} - Command Line Interface'.format(__version__))
    parser.add_argument('--debug', default=False, required=False, help=debug_help, action="store_true")

    args = parser.parse_args()
    pipelinewise = PipelineWise(args, config_dir, venv_dir)
    getattr(pipelinewise, args.command)()

if __name__ == '__main__':
    main()
