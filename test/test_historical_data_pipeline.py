from datetime import date, datetime, timedelta
from unittest.mock import call, patch

import numpy as np
from freezegun import freeze_time

from trazy_analysis.common.constants import DATE_DIR_FORMAT
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.file_storage.common import (
    DATASETS_DIR,
    DONE_DIR,
    ERROR_DIR,
    NONE_DIR,
    TICKERS_DIR,
)
from trazy_analysis.file_storage.meganz_file_storage import MegaNzFileStorage
from trazy_analysis.market_data.historical.historical_data_pipeline import (
    HistoricalDataPipeline,
    TERMINATION_FILE,
    TICKERS_FILE_BASE_NAME,
)
from trazy_analysis.market_data.historical.iex_cloud_historical_data_handler import (
    IexCloudHistoricalDataHandler,
)
from trazy_analysis.market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

TIINGO_HISTORICAL_DATA_HANDLER = TiingoHistoricalDataHandler()
IEX_CLOUD_HISTORICAL_DATA_HANDLER = IexCloudHistoricalDataHandler()
STORAGE = MegaNzFileStorage()
STATUS_CODE_OK = 200
STATUS_CODE_KO = 404
AAPL_SYMBOL = "aapl"
EXCHANGE = "IEX"
AAPL_ASSET = Asset(symbol=AAPL_SYMBOL, exchange=EXCHANGE)


@patch("trazy_analysis.file_storage.meganz_file_storage.MegaNzFileStorage.write")
def test_write_candle_dataframe_in_file_storage(write_mocked):
    candles = np.array(
        [
            Candle(asset=AAPL_ASSET, open=355.15, high=355.15, low=353.74, close=353.84, volume=3254,
                   timestamp=datetime.strptime(
                       "2020-06-17 13:30:00+0000", "%Y-%m-%d %H:%M:%S%z"
                   )),
            Candle(asset=AAPL_ASSET, open=354.28, high=354.96, low=353.96, close=354.78, volume=2324,
                   timestamp=datetime.strptime(
                       "2020-06-18 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z"
                   )),
            Candle(asset=AAPL_ASSET, open=354.92, high=355.32, low=354.09, close=354.09, volume=1123,
                   timestamp=datetime.strptime(
                       "2020-06-18 13:32:00+0000", "%Y-%m-%d %H:%M:%S%z"
                   )),
            Candle(asset=AAPL_ASSET, open=354.25, high=354.59, low=354.14, close=354.59, volume=2613,
                   timestamp=datetime.strptime(
                       "2020-06-19 13:33:00+0000", "%Y-%m-%d %H:%M:%S%z"
                   )),
            Candle(asset=AAPL_ASSET, open=354.22, high=354.26, low=353.95, close=353.98, volume=1186,
                   timestamp=datetime.strptime(
                       "2020-06-19 13:34:00+0000", "%Y-%m-%d %H:%M:%S%z"
                   )),
        ],
        dtype=Candle,
    )
    candle_dataframe = CandleDataFrame.from_candle_list(asset=AAPL_ASSET, candles=candles)
    historical_data_pipeline = HistoricalDataPipeline(
        TIINGO_HISTORICAL_DATA_HANDLER, STORAGE
    )
    historical_data_pipeline.write_candle_dataframe_in_file_storage(
        AAPL_ASSET, candle_dataframe
    )
    expected_dates = ["20200617", "20200618", "20200619"]
    expected_output_filenames = [
        "{}/{}/{}/{}_{}.csv".format(
            DATASETS_DIR,
            expected_dates[0],
            DONE_DIR,
            AAPL_ASSET.key(),
            expected_dates[0],
        ),
        "{}/{}/{}/{}_{}.csv".format(
            DATASETS_DIR,
            expected_dates[1],
            DONE_DIR,
            AAPL_ASSET.key(),
            expected_dates[1],
        ),
        "{}/{}/{}/{}_{}.csv".format(
            DATASETS_DIR,
            expected_dates[2],
            DONE_DIR,
            AAPL_ASSET.key(),
            expected_dates[2],
        ),
    ]
    expected_contents = [
        "timestamp,open,high,low,close,volume\n"
        "2020-06-17 13:30:00+00:00,355.15,355.15,353.74,353.84,3254\n",
        "timestamp,open,high,low,close,volume\n"
        "2020-06-18 13:31:00+00:00,354.28,354.96,353.96,354.78,2324\n"
        "2020-06-18 13:32:00+00:00,354.92,355.32,354.09,354.09,1123\n",
        "timestamp,open,high,low,close,volume\n"
        "2020-06-19 13:33:00+00:00,354.25,354.59,354.14,354.59,2613\n"
        "2020-06-19 13:34:00+00:00,354.22,354.26,353.95,353.98,1186\n",
    ]
    write_mocked_calls = [
        call(expected_output_filenames[0], expected_contents[0]),
        call(expected_output_filenames[1], expected_contents[1]),
        call(expected_output_filenames[2], expected_contents[2]),
    ]
    write_mocked.assert_has_calls(write_mocked_calls)


@patch("trazy_analysis.file_storage.meganz_file_storage.MegaNzFileStorage.ls")
def test_get_todo_dates(ls_mocked):
    historical_data_pipeline = HistoricalDataPipeline(
        TIINGO_HISTORICAL_DATA_HANDLER, STORAGE
    )
    earliest_date = (
        historical_data_pipeline.historical_data_handler.EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD
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


@patch("trazy_analysis.file_storage.meganz_file_storage.MegaNzFileStorage.ls")
def test_get_todo_dates_no_available_date_for_download(ls_mocked):
    historical_data_pipeline = HistoricalDataPipeline(
        TIINGO_HISTORICAL_DATA_HANDLER, STORAGE
    )
    earliest_date = (
        historical_data_pipeline.historical_data_handler.EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD
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


@patch("trazy_analysis.file_storage.meganz_file_storage.MegaNzFileStorage.write")
@patch(
    "trazy_analysis.file_storage.meganz_file_storage.MegaNzFileStorage.create_directory"
)
def test_write_tickers_list_in_file_storage(create_directory_mocked, write_mocked):
    tickers = [
        AAPL_ASSET,
        Asset(symbol="googl", exchange=EXCHANGE),
        Asset(symbol="amzn", exchange=EXCHANGE),
    ]
    date_today = date(2020, 3, 1)

    historical_data_pipeline = HistoricalDataPipeline(
        TIINGO_HISTORICAL_DATA_HANDLER, STORAGE
    )
    historical_data_pipeline.write_tickers_list_in_file_storage(date_today, tickers)

    create_directory_mocked_calls = [call(DATASETS_DIR, TICKERS_DIR)]
    create_directory_mocked.assert_has_calls(create_directory_mocked_calls)

    ticker_filename = "{}/{}/{}_{}.csv".format(
        DATASETS_DIR,
        TICKERS_DIR,
        TICKERS_FILE_BASE_NAME,
        date_today.strftime(DATE_DIR_FORMAT),
    )
    ticker_csv = (
        "tickers\n" "IEX-aapl\n" "IEX-googl\n" "IEX-amzn\n"
    )
    write_mocked_calls = [call(ticker_filename, ticker_csv)]
    write_mocked.assert_has_calls(write_mocked_calls)


@patch("trazy_analysis.file_storage.meganz_file_storage.MegaNzFileStorage.write")
@patch("trazy_analysis.file_storage.meganz_file_storage.MegaNzFileStorage.exists")
def test_handle_states(exists_mocked, write_mocked):
    exists_mocked.side_effect = [True, True, False]

    none_tickers = ["aapl", "googl"]
    errors = [
        ("amzn", "400: Bad Request"),
        ("fb", "401: Unauthorized"),
        ("tsla", "403: Forbidden"),
    ]
    period_dates = [date(2020, 5, 10), date(2020, 5, 11), date(2020, 5, 12)]
    historical_data_pipeline = HistoricalDataPipeline(
        TIINGO_HISTORICAL_DATA_HANDLER, STORAGE
    )
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
    historical_data_pipeline = HistoricalDataPipeline(
        TIINGO_HISTORICAL_DATA_HANDLER, STORAGE
    )
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


@patch(
    "trazy_analysis.market_data.historical.historical_data_pipeline.HistoricalDataPipeline.handle_states"
)
@patch(
    "trazy_analysis.market_data.historical.historical_data_pipeline.HistoricalDataPipeline.write_candle_dataframe_in_file_storage"
)
@patch(
    "trazy_analysis.market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data_from_periods"
)
def test_get_all_tickers_for_all_periods(
    request_ticker_data_from_periods_mocked,
    write_candle_dataframe_in_file_storage_mocked,
    handle_states_mocked,
):
    aapl_candles = np.array(
        [
            Candle(asset=AAPL_ASSET, open=355.15, high=355.15, low=353.74, close=353.84, volume=3254,
                   timestamp=datetime.strptime(
                       "2020-06-17 13:30:00+0000", "%Y-%m-%d %H:%M:%S%z"
                   )),
            Candle(asset=AAPL_ASSET, open=354.28, high=354.96, low=353.96, close=354.78, volume=2324,
                   timestamp=datetime.strptime(
                       "2020-06-18 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z"
                   )),
        ],
        dtype=Candle,
    )
    aapl_candle_dataframe = CandleDataFrame.from_candle_list(asset=AAPL_ASSET, candles=aapl_candles)

    googl_candles = np.array(
        [
            Candle(asset=Asset(symbol="googl", exchange="IEX"), open=354.92, high=355.32, low=354.09, close=354.09,
                   volume=1123, timestamp=datetime.strptime(
                    "2020-06-18 13:32:00+0000", "%Y-%m-%d %H:%M:%S%z"
                )),
            Candle(asset=Asset(symbol="googl", exchange="IEX"), open=354.25, high=354.59, low=354.14, close=354.59,
                   volume=2613, timestamp=datetime.strptime(
                    "2020-06-19 13:33:00+0000", "%Y-%m-%d %H:%M:%S%z"
                )),
        ],
        dtype=Candle,
    )
    googl_candle_dataframe = CandleDataFrame.from_candle_list(asset=Asset(symbol="googl", exchange="IEX"),
                                                              candles=googl_candles)

    amzn_candles = np.array(
        [
            Candle(asset=Asset(symbol="amzn", exchange="IEX"), open=354.22, high=354.26, low=353.95, close=353.98,
                   volume=1186, timestamp=datetime.strptime(
                    "2020-06-19 13:34:00+0000", "%Y-%m-%d %H:%M:%S%z"
                )),
            Candle(asset=Asset(symbol="amzn", exchange="IEX"), open=354.13, high=354.26, low=353.01, close=353.30,
                   volume=1536, timestamp=datetime.strptime(
                    "2020-06-19 13:35:00+0000", "%Y-%m-%d %H:%M:%S%z"
                )),
        ],
        dtype=Candle,
    )
    amzn_candle_dataframe = CandleDataFrame.from_candle_list(asset=Asset(symbol="amzn", exchange="IEX"),
                                                             candles=amzn_candles)

    request_ticker_data_from_periods_mocked.side_effect = [
        (aapl_candle_dataframe, [(date(2020, 6, 11), date(2020, 6, 13))], {}),
        (
            googl_candle_dataframe,
            {(date(2020, 6, 14), date(2020, 6, 16))},
            {(date(2020, 6, 17), date(2020, 6, 19)): "400: Bad Request"},
        ),
        (
            amzn_candle_dataframe,
            [],
            {(date(2020, 6, 14), date(2020, 6, 16)): "400: Bad Request"},
        ),
    ]
    periods = [
        (date(2020, 6, 11), date(2020, 6, 13)),
        (date(2020, 6, 14), date(2020, 6, 16)),
        (date(2020, 6, 17), date(2020, 6, 19)),
    ]
    tickers = ["aapl", "googl", "amzn"]
    historical_data_pipeline = HistoricalDataPipeline(
        TIINGO_HISTORICAL_DATA_HANDLER, STORAGE
    )
    historical_data_pipeline.get_all_tickers_for_all_periods(periods, tickers)

    request_ticker_data_in_range_mocked_calls = [
        call(tickers[0], periods),
        call(tickers[1], periods),
        call(tickers[2], periods),
    ]
    request_ticker_data_from_periods_mocked.assert_has_calls(
        request_ticker_data_in_range_mocked_calls
    )

    write_candle_dataframe_in_file_storage_mocked_calls = [
        call(tickers[0], aapl_candle_dataframe),
        call(tickers[1], googl_candle_dataframe),
        call(tickers[2], amzn_candle_dataframe),
    ]
    write_candle_dataframe_in_file_storage_mocked.assert_has_calls(
        write_candle_dataframe_in_file_storage_mocked_calls
    )

    handle_states_mocked_calls = [
        call(
            [
                date(2020, 6, 11),
                date(2020, 6, 12),
                date(2020, 6, 13),
            ],
            ["aapl"],
            [],
        ),
        call(
            [
                date(2020, 6, 14),
                date(2020, 6, 15),
                date(2020, 6, 16),
            ],
            ["googl"],
            [("amzn", "400: Bad Request")],
        ),
        call(
            [
                date(2020, 6, 17),
                date(2020, 6, 18),
                date(2020, 6, 19),
            ],
            [],
            [("googl", "400: Bad Request")],
        ),
    ]
    handle_states_mocked.assert_has_calls(handle_states_mocked_calls)


@patch(
    "trazy_analysis.market_data.historical.historical_data_pipeline.HistoricalDataPipeline.get_all_tickers_for_all_periods"
)
@patch(
    "trazy_analysis.market_data.historical.historical_data_pipeline.HistoricalDataPipeline.write_tickers_list_in_file_storage"
)
@patch("trazy_analysis.market_data.historical.historical_data_pipeline.get_periods")
@patch(
    "trazy_analysis.file_storage.meganz_file_storage.MegaNzFileStorage.create_all_dates_directories"
)
@patch(
    "trazy_analysis.market_data.historical.historical_data_pipeline.HistoricalDataPipeline.get_todo_dates"
)
@freeze_time("2020-06-18")
def test_start_flow_no_todo_dates(
    get_todo_dates_mocked,
    create_all_dates_directories_mocked,
    get_periods_mocked,
    write_tickers_list_in_file_storage_mocked,
    get_all_tickers_for_all_periods_mocked,
):
    date_today = date(2020, 6, 18)

    get_todo_dates_mocked.return_value = []
    historical_data_pipeline = HistoricalDataPipeline(
        TIINGO_HISTORICAL_DATA_HANDLER, STORAGE
    )
    historical_data_pipeline.start_flow()

    get_todo_dates_mocked_calls = [call(date_today)]
    get_todo_dates_mocked.assert_has_calls(get_todo_dates_mocked_calls)

    create_all_dates_directories_mocked.assert_not_called()
    get_periods_mocked.assert_not_called()
    write_tickers_list_in_file_storage_mocked.assert_not_called()
    get_all_tickers_for_all_periods_mocked.assert_not_called()


@patch(
    "trazy_analysis.market_data.historical.historical_data_pipeline.HistoricalDataPipeline.get_all_tickers_for_all_periods"
)
@patch(
    "trazy_analysis.market_data.historical.historical_data_pipeline.HistoricalDataPipeline.write_tickers_list_in_file_storage"
)
@patch("trazy_analysis.market_data.data_handler.DataHandler.get_tickers_list")
@patch("trazy_analysis.market_data.historical.historical_data_pipeline.get_periods")
@patch(
    "trazy_analysis.file_storage.meganz_file_storage.MegaNzFileStorage.create_all_dates_directories"
)
@patch(
    "trazy_analysis.market_data.historical.historical_data_pipeline.HistoricalDataPipeline.get_todo_dates"
)
@freeze_time("2020-06-18")
def test_start_flow(
    get_todo_dates_mocked,
    create_all_dates_directories_mocked,
    get_periods_mocked,
    get_tickers_list_mocked,
    write_tickers_list_in_file_storage_mocked,
    get_all_tickers_for_all_periods_mocked,
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

    historical_data_pipeline = HistoricalDataPipeline(
        TIINGO_HISTORICAL_DATA_HANDLER, STORAGE
    )
    historical_data_pipeline.start_flow()

    get_todo_dates_mocked_calls = [call(date_today)]
    get_todo_dates_mocked.assert_has_calls(get_todo_dates_mocked_calls)

    create_all_dates_directories_mocked_calls = [call(DATASETS_DIR, todo_dates)]
    create_all_dates_directories_mocked.assert_has_calls(
        create_all_dates_directories_mocked_calls
    )
    start = datetime(2020, 6, 15, 0, 0)
    end = datetime(2020, 6, 19, 23, 59, 59, 999999)
    get_periods_mocked_calls = [
        call(TIINGO_HISTORICAL_DATA_HANDLER.MAX_DOWNLOAD_FRAME, start, end)
    ]
    get_periods_mocked.assert_has_calls(get_periods_mocked_calls)

    write_tickers_list_in_file_storage_mocked_calls = [call(date_today, tickers)]
    write_tickers_list_in_file_storage_mocked.assert_has_calls(
        write_tickers_list_in_file_storage_mocked_calls
    )

    get_all_tickers_for_all_periods_mocked_calls = [call(periods, tickers)]
    get_all_tickers_for_all_periods_mocked.assert_has_calls(
        get_all_tickers_for_all_periods_mocked_calls
    )
