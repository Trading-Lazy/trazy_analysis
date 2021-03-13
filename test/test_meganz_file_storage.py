from pathlib import Path
from unittest.mock import call, patch

import numpy as np
import pytest

from file_storage.common import PATH_SEPARATOR
from file_storage.meganz_file_storage import MegaExtended, MegaNzFileStorage

MEGA_NZ_STORAGE = MegaNzFileStorage()
FILE_CONTENT = "YOLO"
FILE_ID = "dFyGGcKD"
FILE_NAME = "file"
DIR1 = "dir1"
DIR2 = "dir2"
DIR1_ID = "bExFFbJC"


@pytest.fixture
def mega_extended_fixture():
    MEGA_NZ_STORAGE.mega = MegaExtended()


def test_get_id_from_file(mega_extended_fixture):
    file = {
        "h": FILE_ID,
        "p": "GdgFkS4S",
        "u": "nfwBy-E1I9w",
        "t": 0,
        "a": {"n": "tickers_all_20200627.csv"},
        "k": (640959720, 4266667534, 1902818571, 2573311465),
        "s": 90338,
        "ts": 1593247890,
        "iv": (4169159228, 2816634115, 0, 0),
        "meta_mac": (4286136989, 4000806793),
        "key": (
            3736344276,
            1504854797,
            2383673238,
            1997990496,
            4169159228,
            2816634115,
            4286136989,
            4000806793,
        ),
    }
    assert FILE_ID == MEGA_NZ_STORAGE.get_id_from_file(file)


@patch("file_storage.meganz_file_storage.MegaExtended.find")
def test_exists_true(find_mocked, mega_extended_fixture):
    find_response = {
        "h": DIR1_ID,
        "p": "yIxAjYZT",
        "u": "nfwBy-E1I9w",
        "t": 1,
        "a": {"n": "20200626"},
        "k": (1044920321, 444391435, 3900190209, 1001468765),
        "ts": 1593210740,
        "key": (1044920321, 444391435, 3900190209, 1001468765),
    }
    find_mocked.return_value = find_response
    assert MEGA_NZ_STORAGE.exists(DIR1)
    find_calls = [call(DIR1, exclude_deleted=True)]
    find_mocked.assert_has_calls(find_calls)


@patch("file_storage.meganz_file_storage.MegaExtended.find")
def test_exists_false(find_mocked, mega_extended_fixture):
    find_mocked.return_value = None
    assert not MEGA_NZ_STORAGE.exists(FILE_NAME)
    find_calls = [call(FILE_NAME, exclude_deleted=True)]
    find_mocked.assert_has_calls(find_calls)


@patch("file_storage.meganz_file_storage.MegaExtended.find")
def test_ls_not_existing_dir(find_mocked, mega_extended_fixture):
    find_mocked.return_value = None
    expected_list = np.array([], dtype="U256")
    assert (MEGA_NZ_STORAGE.ls(FILE_NAME) == expected_list).all()
    find_calls = [call(FILE_NAME, exclude_deleted=True)]
    find_mocked.assert_has_calls(find_calls)


@patch("file_storage.meganz_file_storage.MegaExtended.get_files_in_node")
@patch("file_storage.meganz_file_storage.MegaExtended.find")
def test_ls_existing_dir(find_mocked, get_files_in_node_mocked, mega_extended_fixture):
    find_mocked.return_value = {
        "h": DIR1_ID,
        "p": "yIxAjYZT",
        "u": "nfwBy-E1I9w",
        "t": 1,
        "a": {"n": "20200529"},
        "k": (1057593020, 3337512258, 2495645871, 2011642809),
        "ts": 1592766084,
        "key": (1057593020, 3337512258, 2495645871, 2011642809),
    }
    get_files_in_node_mocked.return_value = {
        "uQojXKRL": {
            "h": "uQojXKRL",
            "p": "TAwnFYAI",
            "u": "nfwBy-E1I9w",
            "t": 1,
            "a": {"n": "NONE"},
            "k": (1306683732, 100735120, 358731714, 2889731206),
            "ts": 1592766090,
            "key": (1306683732, 100735120, 358731714, 2889731206),
        },
        "rIxBgAhA": {
            "h": "rIxBgAhA",
            "p": "TAwnFYAI",
            "u": "nfwBy-E1I9w",
            "t": 1,
            "a": {"n": "ERROR"},
            "k": (211411669, 2481454825, 2100072242, 2570205662),
            "ts": 1592766097,
            "key": (211411669, 2481454825, 2100072242, 2570205662),
        },
        "DQ41zaaQ": {
            "h": "DQ41zaaQ",
            "p": "TAwnFYAI",
            "u": "nfwBy-E1I9w",
            "t": 1,
            "a": {"n": "DONE"},
            "k": (2125541525, 2339893799, 2122543829, 3853971652),
            "ts": 1592766107,
            "key": (2125541525, 2339893799, 2122543829, 3853971652),
        },
        "jNpiQCDZ": {
            "h": "jNpiQCDZ",
            "p": "TAwnFYAI",
            "u": "nfwBy-E1I9w",
            "t": 0,
            "a": {"n": "terminated.txt"},
            "k": (3041156634, 2225564328, 1519771004, 3586051033),
            "s": 0,
            "ts": 1592771399,
            "iv": (2967766677, 1166507319, 0, 0),
            "meta_mac": (0, 0),
            "key": (
                94427279,
                3240105887,
                1519771004,
                3586051033,
                2967766677,
                1166507319,
                0,
                0,
            ),
        },
    }

    expected_list = np.array(["NONE", "ERROR", "DONE", "terminated.txt"], dtype="U256")
    assert (MEGA_NZ_STORAGE.ls(DIR1) == expected_list).all()

    find_calls = [call(DIR1, exclude_deleted=True)]
    find_mocked.assert_has_calls(find_calls)

    get_files_in_node_calls = [call(DIR1_ID)]
    get_files_in_node_mocked.assert_has_calls(get_files_in_node_calls)


@patch("file_storage.meganz_file_storage.MegaExtended.find")
def test_mkdir_folder_already_exists(find_mocked, mega_extended_fixture):
    find_response = {
        "h": DIR1_ID,
        "p": "yIxAjYZT",
        "u": "nfwBy-E1I9w",
        "t": 1,
        "a": {"n": "20200626"},
        "k": (1044920321, 444391435, 3900190209, 1001468765),
        "ts": 1593210740,
        "key": (1044920321, 444391435, 3900190209, 1001468765),
    }
    find_mocked.return_value = find_response
    MEGA_NZ_STORAGE.mkdir(DIR1)
    find_calls = [call(DIR1, exclude_deleted=True)]
    find_mocked.assert_has_calls(find_calls)


@patch("file_storage.meganz_file_storage.MegaExtended.find")
@patch("file_storage.meganz_file_storage.MegaNzFileStorage.exists")
@patch("file_storage.meganz_file_storage.MegaExtended.create_folder")
def test_mkdir_non_existing_folder(
    create_folder_mocked, exists_mocked, find_mocked, mega_extended_fixture
):
    exists_mocked.side_effect = [False, True, False]
    find_mocked.return_value = {
        "h": DIR1_ID,
        "p": "yIxAjYZT",
        "u": "nfwBy-E1I9w",
        "t": 1,
        "a": {"n": DIR1},
        "k": (1044920321, 444391435, 3900190209, 1001468765),
        "ts": 1593210740,
        "key": (1044920321, 444391435, 3900190209, 1001468765),
    }

    path = str(Path(DIR1) / DIR2)
    MEGA_NZ_STORAGE.mkdir(path)

    find_calls = [call(DIR1 + PATH_SEPARATOR, exclude_deleted=True)]
    find_mocked.assert_has_calls(find_calls)
    exists_calls = [call(path)]
    exists_mocked.assert_has_calls(exists_calls)
    create_folder_calls = [call(DIR2, DIR1_ID)]
    create_folder_mocked.assert_has_calls(create_folder_calls)


@patch("file_storage.meganz_file_storage.MegaExtended.upload")
@patch("file_storage.meganz_file_storage.MegaExtended.find")
@patch("file_storage.meganz_file_storage.MegaExtended.destroy")
@patch("file_storage.meganz_file_storage.MegaNzFileStorage.exists")
def test_write_file_already_exists(
    exists_mocked, destroy_mocked, find_mocked, upload_mocked, mega_extended_fixture
):
    exists_mocked.return_value = True
    find_mocked.side_effect = [
        {
            "h": FILE_ID,
            "p": DIR1_ID,
            "u": "nfwBy-E1I9w",
            "t": 0,
            "a": {"n": FILE_NAME},
            "k": (3148570935, 505106750, 282640317, 2261584137),
            "s": 22509,
            "ts": 1593270376,
            "iv": (4182514774, 2739836998, 0, 0),
            "meta_mac": (341681409, 3336113195),
            "key": (
                1122467169,
                3176515960,
                75831996,
                1075053858,
                4182514774,
                2739836998,
                341681409,
                3336113195,
            ),
        },
        {
            "h": DIR1_ID,
            "p": "HVhXkCzJ",
            "u": "nfwBy-E1I9w",
            "t": 1,
            "a": {"n": "DONE"},
            "k": (1115604771, 248652954, 1752295728, 2697961541),
            "ts": 1593214866,
            "key": (1115604771, 248652954, 1752295728, 2697961541),
        },
    ]
    absolute_path = str(Path(DIR1) / FILE_NAME)
    MEGA_NZ_STORAGE.write(absolute_path, FILE_CONTENT)

    exists_calls = [call(absolute_path)]
    exists_mocked.assert_has_calls(exists_calls)
    find_calls = [
        call(absolute_path, exclude_deleted=True),
        call(DIR1, exclude_deleted=True),
    ]
    find_mocked.assert_has_calls(find_calls)
    destroy_calls = [call(FILE_ID)]
    destroy_mocked.assert_has_calls(destroy_calls)
    upload_calls = [call(absolute_path, FILE_CONTENT, DIR1_ID)]
    upload_mocked.assert_has_calls(upload_calls)


@patch("file_storage.meganz_file_storage.MegaExtended.upload")
@patch("file_storage.meganz_file_storage.MegaExtended.find")
@patch("file_storage.meganz_file_storage.MegaNzFileStorage.exists")
def test_write_non_existing_file(
    exists_mocked, find_mocked, upload_mocked, mega_extended_fixture
):
    exists_mocked.return_value = False
    find_mocked.side_effect = [
        {
            "h": DIR1_ID,
            "p": "HVhXkCzJ",
            "u": "nfwBy-E1I9w",
            "t": 1,
            "a": {"n": "DONE"},
            "k": (1115604771, 248652954, 1752295728, 2697961541),
            "ts": 1593214866,
            "key": (1115604771, 248652954, 1752295728, 2697961541),
        },
    ]
    absolute_path = str(Path(DIR1) / FILE_NAME)
    MEGA_NZ_STORAGE.write(absolute_path, FILE_CONTENT)

    exists_calls = [call(absolute_path)]
    exists_mocked.assert_has_calls(exists_calls)
    find_calls = [call(DIR1, exclude_deleted=True)]
    find_mocked.assert_has_calls(find_calls)
    upload_calls = [call(absolute_path, FILE_CONTENT, DIR1_ID)]
    upload_mocked.assert_has_calls(upload_calls)


@patch("file_storage.meganz_file_storage.MegaExtended.get_file_content")
def test_get_file_content(get_file_content_mocked):
    get_file_content_mocked.return_value = FILE_CONTENT
    assert MEGA_NZ_STORAGE.get_file_content(FILE_NAME) == FILE_CONTENT
    get_file_content_mocked_calls = [call(FILE_NAME)]
    get_file_content_mocked.assert_has_calls(get_file_content_mocked_calls)
