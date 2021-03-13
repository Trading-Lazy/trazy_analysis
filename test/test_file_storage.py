from pathlib import Path
from unittest.mock import call, patch

from file_storage.common import DONE_DIR, ERROR_DIR, NONE_DIR
from file_storage.meganz_file_storage import MegaNzFileStorage

MEGA_NZ_STORAGE = MegaNzFileStorage()
BASE_PATH = "datasets"
DIR = "20200913"


@patch("file_storage.meganz_file_storage.MegaNzFileStorage.mkdir")
def test_create_date_directories(mkdir_mocked):
    path = str(Path(BASE_PATH) / DIR)
    none_dir_path = "{}/{}/".format(path, NONE_DIR)
    error_dir_path = "{}/{}/".format(path, ERROR_DIR)
    done_dir_path = "{}/{}/".format(path, DONE_DIR)
    mkdir_mocked_calls = [
        call(path),
        call(none_dir_path),
        call(error_dir_path),
        call(done_dir_path),
    ]
    MEGA_NZ_STORAGE.create_date_directories(BASE_PATH, DIR)
    mkdir_mocked.assert_has_calls(mkdir_mocked_calls)


@patch("file_storage.meganz_file_storage.MegaNzFileStorage.mkdir")
def test_create_all_dates_directories(mkdir_mocked) -> None:
    dates_strs = ["20200413", "20200414"]
    path1 = str(BASE_PATH / Path(dates_strs[0]))
    none_dir_path1 = "{}/{}/".format(path1, NONE_DIR)
    error_dir_path1 = "{}/{}/".format(path1, ERROR_DIR)
    done_dir_path1 = "{}/{}/".format(path1, DONE_DIR)
    path2 = str(BASE_PATH / Path(dates_strs[1]))
    none_dir_path2 = "{}/{}/".format(path2, NONE_DIR)
    error_dir_path2 = "{}/{}/".format(path2, ERROR_DIR)
    done_dir_path2 = "{}/{}/".format(path2, DONE_DIR)
    mkdir_mocked_calls = [
        call(path1),
        call(none_dir_path1),
        call(error_dir_path1),
        call(done_dir_path1),
        call(path2),
        call(none_dir_path2),
        call(error_dir_path2),
        call(done_dir_path2),
    ]
    MEGA_NZ_STORAGE.create_all_dates_directories(BASE_PATH, dates_strs)
    mkdir_mocked.assert_has_calls(mkdir_mocked_calls)


@patch("file_storage.meganz_file_storage.MegaNzFileStorage.mkdir")
def test_create_directory(mkdir_mocked) -> None:
    path = str(Path(BASE_PATH) / DIR)
    mkdir_mocked_calls = [call(path)]
    MEGA_NZ_STORAGE.create_directory(BASE_PATH, DIR)
    mkdir_mocked.assert_has_calls(mkdir_mocked_calls)
