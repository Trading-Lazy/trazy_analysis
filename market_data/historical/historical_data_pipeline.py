import os
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd

import settings
from common.constants import DATE_DIR_FORMAT
from file_storage.common import (
    DATASETS_DIR,
    DONE_DIR,
    ERROR_DIR,
    NONE_DIR,
    TICKERS_DIR,
    concat_path,
)
from file_storage.file_storage import FileStorage
from logger import logger
from market_data.common import get_periods
from market_data.historical.historical_data_handler import HistoricalDataHandler
from models.asset import Asset

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)
NB_BUSINESS_DAYS = 5
NB_WEEK_DAYS = 7
TERMINATION_FILE = "terminated.txt"
TICKERS_FILE_BASE_NAME = "tickers_all"
LOGS_DIR = "logs"
ONE_HOUR_IN_SECONDS = 3600


class HistoricalDataPipeline:
    def __init__(
        self,
        historical_data_handler: HistoricalDataHandler,
        file_storage: FileStorage,
    ) -> None:
        self.historical_data_handler = historical_data_handler
        self.file_storage = file_storage

    def write_candle_dataframe_in_file_storage(
        self, ticker: Asset, candle_dataframe: str
    ) -> None:
        groups_df = candle_dataframe.groupby(lambda ind: ind.strftime("%Y%m%d"))
        for date_str, group_df in groups_df:
            path = concat_path(DATASETS_DIR, date_str)
            self.file_storage.write(
                "{}/{}/{}_{}.csv".format(path, DONE_DIR, ticker.key(), date_str),
                group_df.to_csv(),
            )

    def get_todo_dates(self, date_today: date) -> List[str]:
        dirs_list = self.file_storage.ls(DATASETS_DIR)
        dates = [
            name
            for name in dirs_list
            if not name.startswith(".") and name != TICKERS_DIR
        ]
        start_date = self.historical_data_handler.EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD
        start_date_str = start_date.strftime(DATE_DIR_FORMAT)
        # get non terminated downloads
        avalaible_dates_for_download = [
            date for date in dates if date >= start_date_str
        ]
        if not avalaible_dates_for_download:
            return []
        LOG.info("Avalaible dates for download: %s", avalaible_dates_for_download)
        non_terminated_downloads = [
            date_dir[0:8]
            for date_dir in avalaible_dates_for_download
            if TERMINATION_FILE
            not in self.file_storage.ls("{}/{}".format(DATASETS_DIR, date_dir))
        ]
        LOG.info("Non terminated downloads: %s", non_terminated_downloads)

        # start date
        last_date = datetime.strptime(
            max(avalaible_dates_for_download)[0:8], DATE_DIR_FORMAT
        ).date()
        start_date = last_date + timedelta(days=1)
        LOG.info("Start date: %s", start_date)

        todo_dates = [
            (start_date + timedelta(days=i)).strftime(DATE_DIR_FORMAT)
            for i in range(0, (date_today - start_date).days + 1)
            if (start_date + timedelta(days=i)).isoweekday() <= NB_BUSINESS_DAYS
        ] + non_terminated_downloads
        todo_dates.sort()
        return todo_dates

    def write_tickers_list_in_file_storage(
        self, date_today: date, tickers: List[Asset]
    ) -> None:
        tickers_df = pd.DataFrame()
        tickers_df["tickers"] = [ticker.key() for ticker in tickers]
        self.file_storage.create_directory(DATASETS_DIR, TICKERS_DIR)
        self.file_storage.write(
            "{}/{}/{}_{}.csv".format(
                DATASETS_DIR,
                TICKERS_DIR,
                TICKERS_FILE_BASE_NAME,
                date_today.strftime(DATE_DIR_FORMAT),
            ),
            tickers_df.to_csv(index=False),
        )

    def handle_states(
        self,
        period_dates: List[date],
        none_tickers: List[str],
        errors: List[Tuple[str, str]],
    ) -> None:
        for date in period_dates:
            date_str = date.strftime(DATE_DIR_FORMAT)
            date_dir_path = "{}/{}".format(DATASETS_DIR, date_str)
            termination_file_path = "{}/{}".format(date_dir_path, TERMINATION_FILE)
            none_dir = "{}/{}".format(date_dir_path, NONE_DIR)
            error_dir = "{}/{}".format(date_dir_path, ERROR_DIR)
            if self.file_storage.exists(date_dir_path):
                self.file_storage.write(termination_file_path, "")
                for none_ticker in none_tickers:
                    self.file_storage.write(
                        "{}/{}_{}.txt".format(none_dir, none_ticker, date_str), ""
                    )
                for error in errors:
                    ticker, error_message = error
                    self.file_storage.write(
                        "{}/{}_{}.txt".format(error_dir, ticker, date_str),
                        error_message,
                    )

    def get_period_dates(self, period: Tuple[date, date]) -> List[date]:
        period_start_date = period[0]
        period_end_date = period[1]
        period_dates = [
            period_start_date + timedelta(days=i)
            for i in range(
                0, (period_end_date - period_start_date + timedelta(days=1)).days
            )
        ]
        return period_dates

    def get_all_tickers_for_all_periods(
        self, periods: List[Tuple[date, date]], tickers: List[str]
    ) -> None:
        none_tickers_per_period: Dict[Tuple[date, date], List[str]] = {}
        errors_per_period: Dict[Tuple[date, date], List[Tuple[str, str]]] = {}
        for ticker in tickers:
            (
                candle_dataframe,
                none_response_periods,
                error_response_periods,
            ) = self.historical_data_handler.request_ticker_data_from_periods(
                ticker, periods
            )

            # group data by date
            self.write_candle_dataframe_in_file_storage(ticker, candle_dataframe)

            # build state lists
            for period in none_response_periods:
                none_tickers_per_period.setdefault(period, []).append(ticker)
            for period, message in error_response_periods.items():
                errors_per_period.setdefault(period, []).append((ticker, message))

        # handle_states
        for period in periods:
            period_dates = self.get_period_dates(period)
            self.handle_states(
                period_dates,
                none_tickers_per_period.get(period, []),
                errors_per_period.get(period, []),
            )

    def start_flow(self) -> None:
        date_today: date = date.today()

        # get remaining dates for downloading data
        todo_dates = self.get_todo_dates(date_today)
        if not todo_dates:
            return
        LOG.info("Todo dates: %s", todo_dates)

        # prepare file_storage and create all needed directories
        self.file_storage.create_all_dates_directories(DATASETS_DIR, todo_dates)

        # generate periods for downloading depending on the api download frame
        start_date = datetime.strptime(todo_dates[0], DATE_DIR_FORMAT).date()
        end_date = datetime.strptime(todo_dates[-1], DATE_DIR_FORMAT).date()
        start = datetime.combine(start_date, datetime.min.time())
        end = datetime.combine(end_date, datetime.max.time())
        periods: List[Tuple[date, date]] = get_periods(
            self.historical_data_handler.MAX_DOWNLOAD_FRAME, start, end
        )
        LOG.info("Periods: %s", periods)

        # get tickers
        tickers = self.historical_data_handler.get_tickers_list()
        self.write_tickers_list_in_file_storage(date_today, tickers)

        # get all tickers data for all periods
        self.get_all_tickers_for_all_periods(periods, tickers)
