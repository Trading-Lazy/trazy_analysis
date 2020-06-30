from historical_data.common import concat_path


def test_concat_path_base_and_complement_empty():
    expected_concatanated_path = ""
    base_path = ""
    complement = ""
    assert expected_concatanated_path == concat_path(base_path, complement)


def test_concat_path_base_empty():
    expected_concatanated_path = "file"
    base_path = ""
    complement = "file"
    assert expected_concatanated_path == concat_path(base_path, complement)


def test_concat_path_complement_empty():
    expected_concatanated_path = "dir"
    base_path = "dir"
    complement = ""
    assert expected_concatanated_path == concat_path(base_path, complement)


def test_concat_path():
    expected_concatanated_path = "dir/file"
    base_path = "dir"
    complement = "file"
    assert expected_concatanated_path == concat_path(base_path, complement)
