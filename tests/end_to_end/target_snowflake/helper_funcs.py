import os
import shutil

from tests.end_to_end.target_snowflake import CONFIG_DIR


def remove_dir(dir_path: str):
    shutil.rmtree(os.path.join(CONFIG_DIR, dir_path), ignore_errors=True)
