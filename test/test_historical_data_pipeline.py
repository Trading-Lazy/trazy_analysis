from datetime import date, timedelta
from unittest.mock import call, patch

from freezegun import freeze_time
from requests import Response

from common.utils import lists_equal
from historical_data.common import (
    DATASETS_DIR,
    DATE_DIR_FORMAT,
    DONE_DIR,
    ERROR_DIR,
    NONE_DIR,
    TICKERS_DIR,
)
from historical_data.historical_data_pipeline import (
    HistoricalDataPipeline,
    TERMINATION_FILE,
    TICKERS_FILE_BASE_NAME,
)
from historical_data.iex_cloud_api_access import IexCloudApiAccess
from historical_data.meganz_storage import MegaNzStorage
from historical_data.tiingo_api_access import TiingoApiAccess

TIINGO_API_ACCESS = TiingoApiAccess()
IEX_CLOUD_API_ACCESS = IexCloudApiAccess()
STORAGE = MegaNzStorage()
STATUS_CODE_OK = 200
STATUS_CODE_KO = 404


@patch("historical_data.meganz_storage.MegaNzStorage.write")
def test_process_ticker_data_tiingo(write_mocked):
    ticker = "aapl"
    data = (
        "date,open,high,low,close,volume\n"
        "2020-06-17 09:30:00-04:00,355.15,355.15,353.74,353.84,3254.0\n"
        "2020-06-18 09:31:00-04:00,354.28,354.96,353.96,354.78,2324.0\n"
        "2020-06-18 09:32:00-04:00,354.92,355.32,354.09,354.09,1123.0\n"
        "2020-06-19 09:33:00-04:00,354.25,354.59,354.14,354.59,2613.0\n"
        "2020-06-19 09:34:00-04:00,354.22,354.26,353.95,353.98,1186.0\n"
    )
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    historical_data_pipeline.process_ticker_data(ticker, data)
    expected_dates = ["20200617", "20200618", "20200619"]
    expected_output_filenames = [
        "{}/{}/{}/{}_{}.csv".format(
            DATASETS_DIR, expected_dates[0], DONE_DIR, ticker, expected_dates[0]
        ),
        "{}/{}/{}/{}_{}.csv".format(
            DATASETS_DIR, expected_dates[1], DONE_DIR, ticker, expected_dates[1]
        ),
        "{}/{}/{}/{}_{}.csv".format(
            DATASETS_DIR, expected_dates[2], DONE_DIR, ticker, expected_dates[2]
        ),
    ]
    expected_contents = [
        "date,open,high,low,close,volume\n"
        "2020-06-17 13:30:00+00:00,355.15,355.15,353.74,353.84,3254.0\n",
        "date,open,high,low,close,volume\n"
        "2020-06-18 13:31:00+00:00,354.28,354.96,353.96,354.78,2324.0\n"
        "2020-06-18 13:32:00+00:00,354.92,355.32,354.09,354.09,1123.0\n",
        "date,open,high,low,close,volume\n"
        "2020-06-19 13:33:00+00:00,354.25,354.59,354.14,354.59,2613.0\n"
        "2020-06-19 13:34:00+00:00,354.22,354.26,353.95,353.98,1186.0\n",
    ]
    write_mocked_calls = [
        call(expected_output_filenames[0], expected_contents[0]),
        call(expected_output_filenames[1], expected_contents[1]),
        call(expected_output_filenames[2], expected_contents[2]),
    ]
    write_mocked.assert_has_calls(write_mocked_calls)


@patch("historical_data.meganz_storage.MegaNzStorage.write")
def test_process_ticker_data_iex(write_mocked):
    ticker = "aapl"
    data = (
        "date,minute,label,high,low,open,close,average,volume,notional,numberOfTrades,symbol\n"
        "2020-06-17,09:30,09:30 AM,192.94,192.6,192.855,192.83,192.804,1362,262599.24,19,AAPL\n"
        "2020-06-18,09:31,09:31 AM,193.27,192.89,192.94,192.9,193.121,2345,452869.29,31,AAPL\n"
        "2020-06-18,09:32,09:32 AM,192.6,192.3,192.6,192.3,192.519,1350,259901,15,AAPL\n"
        "2020-06-19,09:33,09:33 AM,192.46,192.22,192.22,192.29,192.372,756,145432.96,11,AAPL\n"
        "2020-06-19,09:34,09:34 AM,192.89,192.32,192.67,192.89,192.596,1660,319709.6,19,AAPL"
    )
    historical_data_pipeline = HistoricalDataPipeline(IEX_CLOUD_API_ACCESS, STORAGE)
    historical_data_pipeline.process_ticker_data(ticker, data)
    expected_dates = ["20200617", "20200618", "20200619"]
    expected_output_filenames = [
        "{}/{}/{}/{}_{}.csv".format(
            DATASETS_DIR, expected_dates[0], DONE_DIR, ticker, expected_dates[0]
        ),
        "{}/{}/{}/{}_{}.csv".format(
            DATASETS_DIR, expected_dates[1], DONE_DIR, ticker, expected_dates[1]
        ),
        "{}/{}/{}/{}_{}.csv".format(
            DATASETS_DIR, expected_dates[2], DONE_DIR, ticker, expected_dates[2]
        ),
    ]
    expected_contents = [
        "date,high,low,open,close,volume\n"
        "2020-06-17 13:30:00+00:00,192.94,192.6,192.855,192.83,1362\n",
        "date,high,low,open,close,volume\n"
        "2020-06-18 13:31:00+00:00,193.27,192.89,192.94,192.9,2345\n"
        "2020-06-18 13:32:00+00:00,192.6,192.3,192.6,192.3,1350\n",
        "date,high,low,open,close,volume\n"
        "2020-06-19 13:33:00+00:00,192.46,192.22,192.22,192.29,756\n"
        "2020-06-19 13:34:00+00:00,192.89,192.32,192.67,192.89,1660\n",
    ]
    write_mocked_calls = [
        call(expected_output_filenames[0], expected_contents[0]),
        call(expected_output_filenames[1], expected_contents[1]),
        call(expected_output_filenames[2], expected_contents[2]),
    ]
    write_mocked.assert_has_calls(write_mocked_calls)


@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.process_ticker_data"
)
@patch("historical_data.tiingo_api_access.TiingoApiAccess.request_ticker_data")
@patch("requests.Response.content")
def test_get_ticker_csv_tiingo(
    content_mocked, request_ticker_data_mocked, process_ticker_data_mocked
):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = (
        "date,open,high,low,close,volume\n"
        "2020-06-17 09:30:00-04:00,355.15,355.15,353.74,353.84,3254.0\n"
        "2020-06-18 09:31:00-04:00,354.28,354.96,353.96,354.78,2324.0\n"
        "2020-06-18 09:32:00-04:00,354.92,355.32,354.09,354.09,1123.0\n"
        "2020-06-19 09:33:00-04:00,354.25,354.59,354.14,354.59,2613.0\n"
        "2020-06-19 09:34:00-04:00,354.22,354.26,353.95,353.98,1186.0\n"
    )
    request_ticker_data_mocked.return_value.content = str.encode(ticker_data)
    ticker = "aapl"
    period = (date(2020, 6, 17), date(2020, 6, 19))
    none_tickers = []
    errors = []
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    historical_data_pipeline.get_ticker_csv(ticker, period, none_tickers, errors)
    process_ticker_data_mocked_calls = [call(ticker, ticker_data)]
    process_ticker_data_mocked.assert_has_calls(process_ticker_data_mocked_calls)
    expected_none_tickers = []
    expected_errors = []
    assert lists_equal(none_tickers, expected_none_tickers)
    assert lists_equal(errors, expected_errors)


@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.process_ticker_data"
)
@patch("historical_data.iex_cloud_api_access.IexCloudApiAccess.request_ticker_data")
@patch("requests.Response.content")
def test_get_ticker_csv_iex(
    content_mocked, request_ticker_data_mocked, process_ticker_data_mocked
):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = (
        "date,minute,label,high,low,open,close,average,volume,notional,numberOfTrades,symbol\n"
        "2020-06-17,09:30,09:30 AM,192.94,192.6,192.855,192.83,192.804,1362,262599.24,19,AAPL\n"
        "2020-06-18,09:31,09:31 AM,193.27,192.89,192.94,192.9,193.121,2345,452869.29,31,AAPL\n"
        "2020-06-18,09:32,09:32 AM,192.6,192.3,192.6,192.3,192.519,1350,259901,15,AAPL\n"
        "2020-06-19,09:33,09:33 AM,192.46,192.22,192.22,192.29,192.372,756,145432.96,11,AAPL\n"
        "2020-06-19,09:34,09:34 AM,192.89,192.32,192.67,192.89,192.596,1660,319709.6,19,AAPL"
    )
    request_ticker_data_mocked.return_value.content = str.encode(ticker_data)
    ticker = "aapl"
    period = (date(2020, 6, 17), date(2020, 6, 19))
    none_tickers = []
    errors = []
    historical_data_pipeline = HistoricalDataPipeline(IEX_CLOUD_API_ACCESS, STORAGE)
    historical_data_pipeline.get_ticker_csv(ticker, period, none_tickers, errors)
    process_ticker_data_mocked_calls = [call(ticker, ticker_data)]
    process_ticker_data_mocked.assert_has_calls(process_ticker_data_mocked_calls)
    expected_none_tickers = []
    expected_errors = []
    assert lists_equal(none_tickers, expected_none_tickers)
    assert lists_equal(errors, expected_errors)


@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.process_ticker_data"
)
@patch("historical_data.tiingo_api_access.TiingoApiAccess.request_ticker_data")
@patch("requests.Response.content")
def test_get_ticker_csv_none_tiingo(
    content_mocked, request_ticker_data_mocked, process_ticker_data_mocked
):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = ",open,high,low,close,volume\n"
    request_ticker_data_mocked.return_value.content = str.encode(ticker_data)
    ticker = "aapl"
    period = (date(1996, 4, 13), date(1996, 5, 13))
    none_tickers = []
    errors = []
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    historical_data_pipeline.get_ticker_csv(ticker, period, none_tickers, errors)
    expected_none_tickers = ["aapl"]
    expected_errors = []
    assert lists_equal(none_tickers, expected_none_tickers)
    assert lists_equal(errors, expected_errors)


@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.process_ticker_data"
)
@patch("historical_data.iex_cloud_api_access.IexCloudApiAccess.request_ticker_data")
@patch("requests.Response.content")
def test_get_ticker_csv_none_iex(
    content_mocked, request_ticker_data_mocked, process_ticker_data_mocked
):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = ""
    request_ticker_data_mocked.return_value.content = str.encode(ticker_data)
    ticker = "aapl"
    period = (date(1996, 4, 13), date(1996, 5, 13))
    none_tickers = []
    errors = []
    historical_data_pipeline = HistoricalDataPipeline(IEX_CLOUD_API_ACCESS, STORAGE)
    historical_data_pipeline.get_ticker_csv(ticker, period, none_tickers, errors)
    expected_none_tickers = ["aapl"]
    expected_errors = []
    assert lists_equal(none_tickers, expected_none_tickers)
    assert lists_equal(errors, expected_errors)


@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.process_ticker_data"
)
@patch("historical_data.tiingo_api_access.TiingoApiAccess.request_ticker_data")
@patch("requests.Response.content")
def test_get_ticker_csv_error_tiingo(
    content_mocked, request_ticker_data_mocked, process_ticker_data_mocked
):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_KO
    ticker_data = ",open,high,low,close,volume\n"
    error_response = "Not Found"
    request_ticker_data_mocked.return_value.content = str.encode(error_response)
    ticker = "aapl"
    period = (date(1996, 4, 13), date(1996, 5, 13))
    none_tickers = []
    errors = []
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    historical_data_pipeline.get_ticker_csv(ticker, period, none_tickers, errors)
    expected_none_tickers = []
    expected_errors = [("aapl", "404: Not Found")]
    assert lists_equal(none_tickers, expected_none_tickers)
    assert lists_equal(errors, expected_errors)


@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.process_ticker_data"
)
@patch("historical_data.iex_cloud_api_access.IexCloudApiAccess.request_ticker_data")
@patch("requests.Response.content")
def test_get_ticker_csv_error_iex(
    content_mocked, request_ticker_data_mocked, process_ticker_data_mocked
):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_KO
    ticker_data = ""
    error_response = "Not Found"
    request_ticker_data_mocked.return_value.content = str.encode(error_response)
    ticker = "aapl"
    period = (date(1996, 4, 13), date(1996, 5, 13))
    none_tickers = []
    errors = []
    historical_data_pipeline = HistoricalDataPipeline(IEX_CLOUD_API_ACCESS, STORAGE)
    historical_data_pipeline.get_ticker_csv(ticker, period, none_tickers, errors)
    expected_none_tickers = []
    expected_errors = [("aapl", "404: Not Found")]
    assert lists_equal(none_tickers, expected_none_tickers)
    assert lists_equal(errors, expected_errors)


@patch("historical_data.historical_data_pipeline.HistoricalDataPipeline.get_ticker_csv")
def test_get_all_tickers_data(get_ticker_csv_mocked):
    tickers = ["aapl", "googl", "amzn"]
    period = (date(2020, 6, 17), date(2020, 6, 19))
    none_tickers = []
    errors = []
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    historical_data_pipeline.get_all_tickers_data(tickers, period, none_tickers, errors)
    get_ticker_csv_mocked_calls = [
        call(tickers[0], period, none_tickers, errors),
        call(tickers[1], period, none_tickers, errors),
        call(tickers[2], period, none_tickers, errors),
    ]
    get_ticker_csv_mocked.assert_has_calls(get_ticker_csv_mocked_calls)


@patch("historical_data.meganz_storage.MegaNzStorage.ls")
def test_get_todo_dates(ls_mocked):
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    earliest_date = (
        historical_data_pipeline.api_access.EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD
    ) = date(2020, 6, 9)
    dates = [
        (earliest_date + timedelta(days=1)).strftime(DATE_DIR_FORMAT),
        (earliest_date + timedelta(days=2)).strftime(DATE_DIR_FORMAT),
        (earliest_date + timedelta(days=3)).strftime(DATE_DIR_FORMAT),
    ]
    ls_mocked.side_effect = [
        dates,
        [DONE_DIR, ERROR_DIR, NONE_DIR, "terminated.txt"],
        [DONE_DIR, ERROR_DIR, NONE_DIR],
        [DONE_DIR, ERROR_DIR, NONE_DIR, "terminated.txt"],
    ]
    date_today = earliest_date + timedelta(days=10)
    todo_dates = historical_data_pipeline.get_todo_dates(date_today)

    expected_todo_dates = [
        dates[1],
        "20200615",
        "20200616",
        "20200617",
        "20200618",
        "20200619",
    ]
    assert todo_dates == expected_todo_dates
    ls_mocked_calls = [
        call("{}/{}".format(DATASETS_DIR, dates[0])),
        call("{}/{}".format(DATASETS_DIR, dates[1])),
        call("{}/{}".format(DATASETS_DIR, dates[2])),
    ]
    ls_mocked.assert_has_calls(ls_mocked_calls)


@patch("historical_data.meganz_storage.MegaNzStorage.ls")
def test_get_todo_dates_no_available_date_for_download(ls_mocked):
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    earliest_date = (
        historical_data_pipeline.api_access.EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD
    ) = date(2020, 6, 9)
    dates = [
        (earliest_date - timedelta(days=1)).strftime(DATE_DIR_FORMAT),
        (earliest_date - timedelta(days=2)).strftime(DATE_DIR_FORMAT),
        (earliest_date - timedelta(days=3)).strftime(DATE_DIR_FORMAT),
    ]
    ls_mocked.side_effect = [
        dates,
        [DONE_DIR, ERROR_DIR, NONE_DIR, "terminated.txt"],
        [DONE_DIR, ERROR_DIR, NONE_DIR],
        [DONE_DIR, ERROR_DIR, NONE_DIR, "terminated.txt"],
    ]
    date_today = earliest_date + timedelta(days=10)
    todo_dates = historical_data_pipeline.get_todo_dates(date_today)

    expected_todo_dates = []
    assert todo_dates == expected_todo_dates
    ls_mocked_calls = []
    ls_mocked.assert_has_calls(ls_mocked_calls)


def test_get_periods_empty_dates():
    dates = []
    date_today = date(2020, 6, 26)
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    periods = historical_data_pipeline.get_periods(dates, date_today)

    expected_periods = []
    assert periods == expected_periods


def test_get_periods():
    dates = [
        "20200611",
        "20200615",
        "20200616",
        "20200617",
        "20200618",
        "20200619",
        "20200622",
        "20200623",
        "20200624",
        "20200625",
        "20200626",
    ]
    date_today = date(2020, 6, 27)
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    historical_data_pipeline.api_access.MAX_DOWNLOAD_FRAME = timedelta(days=3)
    periods = historical_data_pipeline.get_periods(dates, date_today)

    expected_periods = [
        (date(2020, 6, 11), date(2020, 6, 13)),
        (date(2020, 6, 14), date(2020, 6, 16)),
        (date(2020, 6, 17), date(2020, 6, 19)),
        (date(2020, 6, 20), date(2020, 6, 22)),
        (date(2020, 6, 23), date(2020, 6, 25)),
        (date(2020, 6, 26), date(2020, 6, 26)),
    ]
    assert periods == expected_periods


@patch("historical_data.meganz_storage.MegaNzStorage.write")
@patch("historical_data.meganz_storage.MegaNzStorage.create_directory")
@patch("historical_data.tiingo_api_access.TiingoApiAccess.get_tickers")
def test_get_tickers_list_tiingo(
    get_tickers_mocked, create_directory_mocked, write_mocked
):
    get_tickers_mocked.return_value = ["aapl", "googl", "amzn"]
    date_today = date(2020, 3, 1)

    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    tickers_list = historical_data_pipeline.get_tickers_list(date_today)

    expected_tickers_list = ["aapl", "googl", "amzn"]
    assert tickers_list == expected_tickers_list

    create_directory_mocked_calls = [call(DATASETS_DIR, TICKERS_DIR)]
    create_directory_mocked.assert_has_calls(create_directory_mocked_calls)

    ticker_filename = "{}/{}/{}_{}.csv".format(
        DATASETS_DIR,
        TICKERS_DIR,
        TICKERS_FILE_BASE_NAME,
        date_today.strftime(DATE_DIR_FORMAT),
    )
    ticker_csv = "tickers\n" "aapl\n" "googl\n" "amzn\n"
    write_mocked_calls = [call(ticker_filename, ticker_csv)]
    write_mocked.assert_has_calls(write_mocked_calls)


@patch("historical_data.meganz_storage.MegaNzStorage.write")
@patch("historical_data.meganz_storage.MegaNzStorage.create_directory")
@patch("historical_data.iex_cloud_api_access.IexCloudApiAccess.get_tickers")
def test_get_tickers_list_iex(
    get_tickers_mocked, create_directory_mocked, write_mocked
):
    get_tickers_mocked.return_value = ["aapl", "googl", "amzn"]
    date_today = date(2020, 3, 1)

    historical_data_pipeline = HistoricalDataPipeline(IEX_CLOUD_API_ACCESS, STORAGE)
    tickers_list = historical_data_pipeline.get_tickers_list(date_today)

    expected_tickers_list = ["aapl", "googl", "amzn"]
    assert tickers_list == expected_tickers_list

    create_directory_mocked_calls = [call(DATASETS_DIR, TICKERS_DIR)]
    create_directory_mocked.assert_has_calls(create_directory_mocked_calls)

    ticker_filename = "{}/{}/{}_{}.csv".format(
        DATASETS_DIR,
        TICKERS_DIR,
        TICKERS_FILE_BASE_NAME,
        date_today.strftime(DATE_DIR_FORMAT),
    )
    ticker_csv = "tickers\n" "aapl\n" "googl\n" "amzn\n"
    write_mocked_calls = [call(ticker_filename, ticker_csv)]
    write_mocked.assert_has_calls(write_mocked_calls)


@patch("historical_data.meganz_storage.MegaNzStorage.write")
@patch("historical_data.meganz_storage.MegaNzStorage.exists")
def test_handle_states(exists_mocked, write_mocked):
    exists_mocked.side_effect = [True, True, False]

    none_tickers = ["aapl", "googl"]
    errors = [
        ("amzn", "400: Bad Request"),
        ("fb", "401: Unauthorized"),
        ("tsla", "403: Forbidden"),
    ]
    period_dates = [date(2020, 5, 10), date(2020, 5, 11), date(2020, 5, 12)]
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    historical_data_pipeline.handle_states(period_dates, none_tickers, errors)

    dates_str = [date.strftime(DATE_DIR_FORMAT) for date in period_dates]
    exists_mocked_calls = [
        call("{}/{}".format(DATASETS_DIR, dates_str[0])),
        call("{}/{}".format(DATASETS_DIR, dates_str[1])),
        call("{}/{}".format(DATASETS_DIR, dates_str[2])),
    ]
    exists_mocked.assert_has_calls(exists_mocked_calls)

    dates_dir_path = ["{}/{}".format(DATASETS_DIR, date_str) for date_str in dates_str]
    write_mocked_calls = [
        call("{}/{}".format(dates_dir_path[0], TERMINATION_FILE), ""),
        call(
            "{}/{}/{}_{}.txt".format(
                dates_dir_path[0], NONE_DIR, none_tickers[0], dates_str[0]
            ),
            "",
        ),
        call(
            "{}/{}/{}_{}.txt".format(
                dates_dir_path[0], NONE_DIR, none_tickers[1], dates_str[0]
            ),
            "",
        ),
        call(
            "{}/{}/{}_{}.txt".format(
                dates_dir_path[0], ERROR_DIR, errors[0][0], dates_str[0]
            ),
            errors[0][1],
        ),
        call(
            "{}/{}/{}_{}.txt".format(
                dates_dir_path[0], ERROR_DIR, errors[1][0], dates_str[0]
            ),
            errors[1][1],
        ),
        call(
            "{}/{}/{}_{}.txt".format(
                dates_dir_path[0], ERROR_DIR, errors[2][0], dates_str[0]
            ),
            errors[2][1],
        ),
        call("{}/{}".format(dates_dir_path[1], TERMINATION_FILE), ""),
        call(
            "{}/{}/{}_{}.txt".format(
                dates_dir_path[1], NONE_DIR, none_tickers[0], dates_str[1]
            ),
            "",
        ),
        call(
            "{}/{}/{}_{}.txt".format(
                dates_dir_path[1], NONE_DIR, none_tickers[1], dates_str[1]
            ),
            "",
        ),
        call(
            "{}/{}/{}_{}.txt".format(
                dates_dir_path[1], ERROR_DIR, errors[0][0], dates_str[1]
            ),
            errors[0][1],
        ),
        call(
            "{}/{}/{}_{}.txt".format(
                dates_dir_path[1], ERROR_DIR, errors[1][0], dates_str[1]
            ),
            errors[1][1],
        ),
        call(
            "{}/{}/{}_{}.txt".format(
                dates_dir_path[1], ERROR_DIR, errors[2][0], dates_str[1]
            ),
            errors[2][1],
        ),
    ]
    write_mocked.assert_has_calls(write_mocked_calls)


def test_get_period_dates():
    period = (date(2020, 5, 10), date(2020, 5, 17))
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    period_dates = historical_data_pipeline.get_period_dates(period)
    expected_period_dates = [
        date(2020, 5, 10),
        date(2020, 5, 11),
        date(2020, 5, 12),
        date(2020, 5, 13),
        date(2020, 5, 14),
        date(2020, 5, 15),
        date(2020, 5, 16),
        date(2020, 5, 17),
    ]
    assert period_dates == expected_period_dates


@patch("historical_data.historical_data_pipeline.HistoricalDataPipeline.handle_states")
@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.get_period_dates"
)
@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.get_all_tickers_data"
)
def test_get_tickers_for_all_periods(
    get_all_tickers_data_mocked, get_period_dates_mocked, handle_states_mocked
):
    get_period_dates_mocked.side_effect = [
        [date(2020, 6, 11), date(2020, 6, 12), date(2020, 6, 13)],
        [date(2020, 6, 14), date(2020, 6, 15), date(2020, 6, 16)],
        [date(2020, 6, 17), date(2020, 6, 18), date(2020, 6, 19)],
    ]

    periods = [
        (date(2020, 6, 11), date(2020, 6, 13)),
        (date(2020, 6, 14), date(2020, 6, 16)),
        (date(2020, 6, 17), date(2020, 6, 19)),
    ]
    tickers = ["aapl", "googl", "amzn"]
    date_today = date(2020, 6, 18)
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    historical_data_pipeline.get_tickers_for_all_periods(periods, tickers, date_today)

    none_tickers = []
    errors = []
    get_all_tickers_data_mocked_calls = [
        call(tickers, periods[0], none_tickers, errors),
        call(tickers, periods[1], none_tickers, errors),
        call(tickers, periods[2], none_tickers, errors),
    ]
    get_all_tickers_data_mocked.assert_has_calls(get_all_tickers_data_mocked_calls)

    get_period_dates_mocked_calls = [
        call(periods[0]),
        call(periods[1]),
        call(periods[2]),
    ]
    get_period_dates_mocked.assert_has_calls(get_period_dates_mocked_calls)

    handle_states_mocked_calls = [
        call(
            [date(2020, 6, 11), date(2020, 6, 12), date(2020, 6, 13)],
            none_tickers,
            errors,
        ),
        call(
            [date(2020, 6, 14), date(2020, 6, 15), date(2020, 6, 16)],
            none_tickers,
            errors,
        ),
        call([date(2020, 6, 17), date(2020, 6, 18)], none_tickers, errors),
    ]
    handle_states_mocked.assert_has_calls(handle_states_mocked_calls)


@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.get_tickers_for_all_periods"
)
@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.get_tickers_list"
)
@patch("historical_data.historical_data_pipeline.HistoricalDataPipeline.get_periods")
@patch("historical_data.meganz_storage.MegaNzStorage.create_all_dates_directories")
@patch("historical_data.historical_data_pipeline.HistoricalDataPipeline.get_todo_dates")
@freeze_time("2020-06-18")
def test_start_flow_no_todo_dates(
    get_todo_dates_mocked,
    create_all_dates_directories_mocked,
    get_periods_mocked,
    get_tickers_list_mocked,
    get_tickers_for_all_periods_mocked,
):
    date_today = date(2020, 6, 18)

    get_todo_dates_mocked.return_value = []
    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    historical_data_pipeline.start_flow()

    get_todo_dates_mocked_calls = [call(date_today)]
    get_todo_dates_mocked.assert_has_calls(get_todo_dates_mocked_calls)

    create_all_dates_directories_mocked.assert_not_called()
    get_periods_mocked.assert_not_called()
    get_tickers_list_mocked.assert_not_called()
    get_tickers_for_all_periods_mocked.assert_not_called()


@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.get_tickers_for_all_periods"
)
@patch(
    "historical_data.historical_data_pipeline.HistoricalDataPipeline.get_tickers_list"
)
@patch("historical_data.historical_data_pipeline.HistoricalDataPipeline.get_periods")
@patch("historical_data.meganz_storage.MegaNzStorage.create_all_dates_directories")
@patch("historical_data.historical_data_pipeline.HistoricalDataPipeline.get_todo_dates")
@freeze_time("2020-06-18")
def test_start_flow(
    get_todo_dates_mocked,
    create_all_dates_directories_mocked,
    get_periods_mocked,
    get_tickers_list_mocked,
    get_tickers_for_all_periods_mocked,
):
    date_today = date(2020, 6, 18)

    todo_dates = [
        "20200615",
        "20200616",
        "20200617",
        "20200618",
        "20200619",
    ]
    get_todo_dates_mocked.return_value = todo_dates

    periods = [
        (date(2020, 6, 15), date(2020, 6, 17)),
        (date(2020, 6, 18), date(2020, 6, 19)),
    ]
    get_periods_mocked.return_value = periods

    tickers = ["aapl", "googl", "amzn"]
    get_tickers_list_mocked.return_value = tickers

    historical_data_pipeline = HistoricalDataPipeline(TIINGO_API_ACCESS, STORAGE)
    historical_data_pipeline.start_flow()

    get_todo_dates_mocked_calls = [call(date_today)]
    get_todo_dates_mocked.assert_has_calls(get_todo_dates_mocked_calls)

    create_all_dates_directories_mocked_calls = [call(DATASETS_DIR, todo_dates)]
    create_all_dates_directories_mocked.assert_has_calls(
        create_all_dates_directories_mocked_calls
    )

    get_periods_mocked_calls = [call(todo_dates, date_today)]
    get_periods_mocked.assert_has_calls(get_periods_mocked_calls)

    get_tickers_list_mocked_calls = [call(date_today)]
    get_tickers_list_mocked.assert_has_calls(get_tickers_list_mocked_calls)

    get_tickers_for_all_periods_mocked_calls = [call(periods, tickers, date_today)]
    get_tickers_for_all_periods_mocked.assert_has_calls(
        get_tickers_for_all_periods_mocked_calls
    )
