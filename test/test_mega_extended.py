from pathlib import Path
from unittest.mock import call, patch

import pytest

from file_storage.meganz_file_storage import MegaExtended

MEGA_EXTENDED: MegaExtended = MegaExtended()
EMAIL: str = "email"
PASSWORD: str = "password"
DIR1: str = "dir1"
DIR1_ID: str = "bExFFbJC"
DIR2: str = "dir2"

FILE1_CONTENT: str = "YOLO"
FILE1_ID: str = "dFyGGcKD"
FILE1_NAME: str = "file1"
FILE1: dict = {
    "h": FILE1_ID,
    "t": 0,
    "a": {"n": FILE1_NAME},
    "k": "nfwBy-E1I9w:zaLq4SBmRYtTHrFy0GMnIoIvzlcFWLLVZ_u4-17Tysg",
    "p": DIR1_ID,
    "ts": 1593247890,
    "u": "nfwBy-E1I9w",
    "s": 90338,
}

FILE2_CONTENT: str = "TOTO"
FILE2_ID: str = "dFyGGcKE"
FILE2_NAME: str = "file2"
FILE2: dict = {
    "h": FILE2_ID,
    "t": 0,
    "a": {"n": FILE2_NAME},
    "k": "nfwBy-E1I9w:zaLq4SBmRYtTHrFy0GMnIoIvzlcFWLLVZ_u4-17Tysg",
    "p": DIR1_ID,
    "ts": 1593247890,
    "u": "nfwBy-E1I9w",
    "s": 90338,
}

FILE3_DATA: dict = {
    "s": 21731,
    "at": "x5XOgmzhPYcu0X0jVnthFhOn4OO2Y32D--Wb_njzvKQ",
    "msd": 1,
    "tl": 0,
    "g": "http://gfs270n161.userstorage.mega.co.nz/dl/z2lGiF-M_Z_oKAJmoMCqe86MqozUWpSB227089H0smugbDRp1beldJaH7uOcIsN1RrppYfKbKVKtz_vH-AIZWHMiEj8hSIxyWPBDH_gzLJ7Fo0uxJAkHWA",
    "pfa": 1,
}
FILE3: dict = {
    "h": "rEhgnKaB",
    "p": "mUg3zLAA",
    "u": "nfwBy-E1I9w",
    "t": 0,
    "a": {"n": "A_20200619.csv"},
    "k": (3138063493, 4138691548, 4110416790, 950858003),
    "s": 21731,
    "ts": 1593480517,
    "iv": (483906093, 760825075, 0, 0),
    "meta_mac": (488697727, 3369973217),
    "key": (
        2816269992,
        3690344239,
        3923711209,
        4033955058,
        483906093,
        760825075,
        488697727,
        3369973217,
    ),
}

TRASH_ID = "dsVLSV34"
TRASH_NAME = "trash"
FILE_IN_TRASH_ID: str = "dFyGGcKF"
FILE_IN_TRASH_NAME: str = "file_in_trash"
FILE_IN_TRASH: dict = {
    "h": FILE_IN_TRASH_ID,
    "t": 0,
    "a": {"n": FILE_IN_TRASH_NAME},
    "k": "nfwBy-E1I9w:zaLq4SBmRYtTHrFy0GMnIoIvzlcFWLLVZ_u4-17Tysg",
    "p": TRASH_ID,
    "ts": 1593247890,
    "u": "nfwBy-E1I9w",
    "s": 90338,
}

ENCODING: str = "utf8"


@pytest.fixture
def mega_extended_fixture():
    MEGA_EXTENDED.path_cache = {}
    MEGA_EXTENDED.id_cache = {}


@patch("mega.Mega.login")
def test_login(login_mocked, mega_extended_fixture):
    MEGA_EXTENDED.login(EMAIL, PASSWORD)
    calls = [call(EMAIL, PASSWORD)]
    login_mocked.assert_has_calls(calls)


@patch("mega.Mega.get_files")
def test_get_files(get_files_mocked, mega_extended_fixture):
    MEGA_EXTENDED.get_files()
    calls = [call()]
    get_files_mocked.assert_has_calls(calls)


@patch("mega.Mega.get_files_in_node")
def test_get_files_in_node(get_files_in_node_mocked, mega_extended_fixture):
    MEGA_EXTENDED.get_files_in_node(FILE1_NAME)
    calls = [call(FILE1_NAME)]
    get_files_in_node_mocked.assert_has_calls(calls)


@patch("mega.Mega.create_folder")
def test_create_folder_helper(create_folder_mocked, mega_extended_fixture):
    MEGA_EXTENDED.create_folder_helper(FILE1_NAME, DIR1)
    calls = [call(FILE1_NAME, DIR1)]
    create_folder_mocked.assert_has_calls(calls)


def test_find_cached_file(mega_extended_fixture):
    MEGA_EXTENDED.path_cache[FILE1_NAME] = FILE1
    assert MEGA_EXTENDED.find(FILE1_NAME) == FILE1


@patch("mega.Mega.get_files")
def test_find_with_handle(get_files_mocked, mega_extended_fixture):
    get_files_mocked.return_value = {FILE1_ID: FILE1}
    assert MEGA_EXTENDED.find(handle=FILE1_ID) == FILE1


@patch("mega.Mega.get_files")
def test_find_file_at_root(get_files_mocked, mega_extended_fixture):
    get_files_mocked.return_value = {FILE2_ID: FILE2, FILE1_ID: FILE1}
    assert MEGA_EXTENDED.find(FILE1_NAME) == FILE1


@patch("mega.Mega.get_files")
@patch("mega.Mega.find_path_descriptor")
def test_find_exclude_deleted(find_path_descriptor_mocked, get_files_mocked):
    MEGA_EXTENDED._trash_folder_node_id = TRASH_ID
    path = str(Path(TRASH_NAME) / FILE_IN_TRASH_NAME)
    get_files_mocked.return_value = {FILE2_ID: FILE2, FILE_IN_TRASH_ID: FILE_IN_TRASH}
    find_path_descriptor_mocked.side_effect = ["", TRASH_ID]
    assert MEGA_EXTENDED.find(path, exclude_deleted=True) == None


@patch("mega.Mega.get_files")
@patch("mega.Mega.find_path_descriptor")
def test_find_file_in_subdirectory(
    find_path_descriptor_mocked, get_files_mocked, mega_extended_fixture
):
    path = str(Path(DIR1) / FILE1_NAME)
    get_files_mocked.return_value = {FILE2_ID: FILE2, FILE1_ID: FILE1}
    find_path_descriptor_mocked.side_effect = ["", DIR1_ID]
    assert MEGA_EXTENDED.find(path) == FILE1


@patch("mega.Mega.get_files")
@patch("mega.Mega.find_path_descriptor")
def test_find_not_existing_file(
    find_path_descriptor_mocked, get_files_mocked, mega_extended_fixture
):
    path = str(Path(DIR1) / FILE1_NAME)
    get_files_mocked.return_value = {FILE2_ID: FILE2, FILE1_ID: FILE1}
    find_path_descriptor_mocked.side_effect = ["", ""]
    assert MEGA_EXTENDED.find(path) == None


@patch("file_storage.meganz_file_storage.MegaExtended.find")
@patch("file_storage.meganz_file_storage.MegaExtended.create_folder_helper")
def test_create_folder_none_dest(
    create_folder_helper_mocked, find_mocked, mega_extended_fixture
):
    create_folder_helper_response = {DIR1: DIR1_ID}
    create_folder_helper_mocked.return_value = create_folder_helper_response

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
    created_folder_response = MEGA_EXTENDED.create_folder(DIR1)

    created_folder_helper_calls = [call(DIR1, None)]
    create_folder_helper_mocked.assert_has_calls(created_folder_helper_calls)

    find_response_file_id = DIR1_ID
    find_calls = [call(handle=find_response_file_id)]
    find_mocked.assert_has_calls(find_calls)

    assert MEGA_EXTENDED.path_cache[DIR1] == find_response
    assert MEGA_EXTENDED.id_cache[find_response_file_id] == DIR1
    assert created_folder_response == create_folder_helper_response


@patch("file_storage.meganz_file_storage.MegaExtended.find")
@patch("file_storage.meganz_file_storage.MegaExtended.create_folder_helper")
def test_create_folder_dest(
    create_folder_helper_mocked, find_mocked, mega_extended_fixture
):
    create_folder_helper_response = {DIR1: DIR1_ID}
    create_folder_helper_mocked.return_value = create_folder_helper_response

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
    created_folder_info = MEGA_EXTENDED.create_folder(DIR1, DIR2)

    created_folder_helper_calls = [call(DIR1, DIR2)]
    create_folder_helper_mocked.assert_has_calls(created_folder_helper_calls)

    find_response_file_id = DIR1_ID
    find_calls = [call(handle=find_response_file_id)]
    find_mocked.assert_has_calls(find_calls)

    path_str = str(Path(DIR2) / DIR1)
    assert MEGA_EXTENDED.path_cache[path_str] == find_response
    assert MEGA_EXTENDED.id_cache[find_response_file_id] == path_str
    assert created_folder_info == create_folder_helper_response


def test_upload_helper(mega_extended_fixture):
    pass


@patch("file_storage.meganz_file_storage.MegaExtended.find")
@patch("file_storage.meganz_file_storage.MegaExtended.upload_helper")
def test_upload(upload_helper_mocked, find_mocked, mega_extended_fixture):
    upload_helper_response = {
        "f": [
            {
                "h": FILE1_ID,
                "t": 0,
                "a": "jU3tSYjsvKMtET7EsH98BpLa17ps--VGQUTlqkDmR42t7gKaLY3mu5BHPgUvaf5b",
                "k": "nfwBy-E1I9w:zaLq4SBmRYtTHrFy0GMnIoIvzlcFWLLVZ_u4-17Tysg",
                "p": "GdgFkS4S",
                "ts": 1593247890,
                "u": "nfwBy-E1I9w",
                "s": 90338,
            }
        ]
    }
    upload_helper_mocked.return_value = upload_helper_response

    find_response = {
        "h": FILE1_ID,
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
    find_mocked.return_value = find_response
    absolute_path = str(Path(DIR1) / FILE1_NAME)
    upload_response = MEGA_EXTENDED.upload(absolute_path, FILE2_CONTENT, DIR1_ID)

    upload_helper_calls = [call(absolute_path, FILE2_CONTENT, DIR1_ID, None, ENCODING)]
    upload_helper_mocked.assert_has_calls(upload_helper_calls)

    find_response_file_id = FILE1_ID
    assert MEGA_EXTENDED.path_cache[absolute_path] == find_response
    assert MEGA_EXTENDED.id_cache[find_response_file_id] == absolute_path
    assert upload_response == upload_helper_response


@patch("mega.Mega.destroy")
def test_destroy_helper(destroy_mocked, mega_extended_fixture):
    MEGA_EXTENDED.destroy_helper(FILE1_ID)
    calls = [call(FILE1_ID)]
    destroy_mocked.assert_has_calls(calls)


@patch("file_storage.meganz_file_storage.MegaExtended.destroy_helper")
def test_destroy(destroy_helper_mocked, mega_extended_fixture):
    destroy_helper_response = 0
    destroy_helper_mocked.return_value = destroy_helper_response

    MEGA_EXTENDED.path_cache[FILE1_NAME] = (
        "WAph1Y5Q",
        {
            "h": "WAph1Y5Q",
            "p": "aVoyEAyB",
            "u": "nfwBy-E1I9w",
            "t": 0,
            "a": {"n": "A_20200623.csv"},
            "k": (377385959, 3061695077, 1110585282, 2279529166),
            "s": 22013,
            "ts": 1593211581,
            "iv": (1781034388, 1054646528, 0, 0),
            "meta_mac": (3118213197, 3186904584),
            "key": (
                2086018675,
                2292278117,
                4226680719,
                975869126,
                1781034388,
                1054646528,
                3118213197,
                3186904584,
            ),
        },
    )
    MEGA_EXTENDED.id_cache[FILE1_ID] = FILE1_NAME
    destroy_response = MEGA_EXTENDED.destroy(FILE1_ID)

    destroy_helper_calls = [call(FILE1_ID)]
    destroy_helper_mocked.assert_has_calls(destroy_helper_calls)

    assert destroy_response == destroy_helper_response
    assert FILE1_NAME not in MEGA_EXTENDED.path_cache
    assert FILE1_ID not in MEGA_EXTENDED.id_cache


@patch("file_storage.meganz_file_storage.MegaExtended.get_file_content_helper")
def test_get_file_content(get_file_content_helper_mocked):
    get_file_content_helper_mocked.return_value = FILE1_CONTENT
    assert MEGA_EXTENDED.get_file_content(FILE1_NAME) == FILE1_CONTENT
    get_file_content_helper_mocked_calls = [call(FILE1_NAME)]
    get_file_content_helper_mocked.assert_has_calls(
        get_file_content_helper_mocked_calls
    )
