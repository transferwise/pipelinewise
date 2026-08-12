import os

from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from tests.end_to_end.helpers import env as env_module
from tests.end_to_end.helpers.env import E2EEnv


class EndToEndTempCleanupHelpersTestCase(TestCase):
    """Tests for E2E staging cleanup that do not require external services."""

    def test_temp_cleanup_removes_files_and_unlinks_directory_symlinks(self):
        """Clean files and links without recursively deleting a link target."""
        with TemporaryDirectory() as temp_directory:
            temp_file = os.path.join(temp_directory, 'part.csv.gz')
            target_directory = os.path.join(temp_directory, 'source-data')
            directory_symlink = os.path.join(temp_directory, 'source-link')
            os.mkdir(target_directory)
            with open(temp_file, 'w', encoding='utf-8') as file_handle:
                file_handle.write('staged data')
            os.symlink(target_directory, directory_symlink)

            with mock.patch.object(
                env_module.glob,
                'glob',
                return_value=[temp_file, directory_symlink],
            ):
                E2EEnv.clean_up_temp_dir()

            self.assertFalse(os.path.exists(temp_file))
            self.assertFalse(os.path.lexists(directory_symlink))
            self.assertTrue(os.path.isdir(target_directory))

    def test_temp_cleanup_removes_directories_recursively(self):
        """Clean nested connector staging directories left by E2E runs."""
        with TemporaryDirectory() as temp_directory:
            staging_directory = os.path.join(temp_directory, 'mongo_source_db')
            os.mkdir(staging_directory)
            with open(
                os.path.join(staging_directory, 'document.json'),
                'w',
                encoding='utf-8',
            ) as file_handle:
                file_handle.write('{}')

            with mock.patch.object(
                env_module.glob, 'glob', return_value=[staging_directory]
            ):
                E2EEnv.clean_up_temp_dir()

            self.assertFalse(os.path.exists(staging_directory))

    def test_temp_cleanup_tolerates_a_concurrently_removed_entry(self):
        """A file disappearing between discovery and deletion is clean state."""
        temp_file = os.path.join(env_module.CONFIG_DIR, 'tmp', 'part.csv.gz')

        with mock.patch.object(
            env_module.glob, 'glob', return_value=[temp_file]
        ), mock.patch.object(
            env_module.os, 'remove', side_effect=FileNotFoundError
        ) as remove_mock:
            E2EEnv.clean_up_temp_dir()

        remove_mock.assert_called_once_with(temp_file)

    def test_temp_cleanup_propagates_file_removal_failures(self):
        """A file-removal error must not leave stale staged data silently."""
        temp_file = os.path.join(env_module.CONFIG_DIR, 'tmp', 'part.csv.gz')

        with mock.patch.object(
            env_module.glob, 'glob', return_value=[temp_file]
        ), mock.patch.object(
            env_module.os,
            'remove',
            side_effect=PermissionError('cleanup failed'),
        ):
            with self.assertRaisesRegex(PermissionError, 'cleanup failed'):
                E2EEnv.clean_up_temp_dir()

    def test_temp_cleanup_propagates_directory_removal_failures(self):
        """A directory-removal error must not leave staged data silently."""
        with TemporaryDirectory() as temp_directory:
            staging_directory = os.path.join(temp_directory, 'mongo_source_db')
            os.mkdir(staging_directory)

            with mock.patch.object(
                env_module.glob, 'glob', return_value=[staging_directory]
            ), mock.patch.object(
                env_module.shutil,
                'rmtree',
                side_effect=PermissionError('cleanup failed'),
            ):
                with self.assertRaisesRegex(PermissionError, 'cleanup failed'):
                    E2EEnv.clean_up_temp_dir()
