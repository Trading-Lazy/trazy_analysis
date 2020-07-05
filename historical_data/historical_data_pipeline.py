from datetime import date, datetime, timedelta
from typing import List, Tuple

import pandas as pd

from historical_data.common import (
    DATASETS_DIR,
    DATE_DIR_FORMAT,
    DONE_DIR,
    ENCODING,
    ERROR_DIR,
    LOG,
    NONE_DIR,
    TICKERS_DIR,
    concat_path,
)
from historical_data.historical_data_api_access import HistoricalDataApiAccess
from historical_data.storage import Storage

NB_BUSINESS_DAYS = 5
NB_WEEK_DAYS = 7
TERMINATION_FILE = "terminated.txt"
TICKERS_FILE_BASE_NAME = "tickers_all"
LOGS_DIR = "logs"
ONE_HOUR_IN_SECONDS = 3600


class HistoricalDataPipeline:
    def __init__(self, api_access: HistoricalDataApiAccess, storage: Storage,) -> None:
        self.api_access = api_access
        self.storage = storage

    def process_ticker_data(self, ticker: str, data: str) -> None:
        LOG.info("Parsing ticker data")
        groups_df = self.api_access.parse_ticker_data(data)
        LOG.info("Parsing succeeded")
        LOG.info("Write in storage")
        for date_str, group_df in groups_df:
            path = concat_path(DATASETS_DIR, date_str)
            self.storage.write(
                "{}/{}/{}_{}.csv".format(path, DONE_DIR, ticker, date_str),
                group_df.to_csv(),
            )

    def get_ticker_csv(
        self,
        ticker: str,
        period: Tuple[date, date],
        none_tickers: List[str],
        errors: List[Tuple[str, str]],
    ) -> None:
        response = self.api_access.request_ticker_data(ticker, period)
        data: str = response.content.decode(ENCODING)
        if response:
            if not self.api_access.ticker_data_is_none(data):
                LOG.info("Ticker data is not none. Downloading data")
                self.process_ticker_data(ticker, data)
            else:
                LOG.info("Ticker data is none")
                none_tickers.append(ticker)
        else:
            errors.append((ticker, "{}: {}".format(response.status_code, data)))

    def get_all_tickers_data(
        self,
        tickers: List[str],
        period: Tuple[date, date],
        none_tickers: List[str],
        errors: List[Tuple[str, str]],
    ) -> None:
        for ticker in tickers:
            self.get_ticker_csv(ticker, period, none_tickers, errors)

    def get_todo_dates(self, date_today: date) -> List[str]:
        dirs_list = self.storage.ls(DATASETS_DIR)
        dates = [
            name
            for name in dirs_list
            if not name.startswith(".") and name != TICKERS_DIR
        ]
        start_date = self.api_access.EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD
        start_date_str = start_date.strftime(DATE_DIR_FORMAT)
        # get non terminated downloads
        avalaible_dates_for_download = [
            date for date in dates if date >= start_date_str
        ]
        if not avalaible_dates_for_download:
            return []
        LOG.info(
            "Avalaible dates for download: {}".format(avalaible_dates_for_download)
        )
        non_terminated_downloads = [
            date_dir[0:8]
            for date_dir in avalaible_dates_for_download
            if TERMINATION_FILE
            not in self.storage.ls("{}/{}".format(DATASETS_DIR, date_dir))
        ]
        LOG.info("Non terminated downloads: {}".format(non_terminated_downloads))

        # start date
        last_date = datetime.strptime(
            max(avalaible_dates_for_download)[0:8], DATE_DIR_FORMAT
        ).date()
        start_date = last_date + timedelta(days=1)
        LOG.info("Start date: {}".format(start_date))

        todo_dates = [
            (start_date + timedelta(days=i)).strftime(DATE_DIR_FORMAT)
            for i in range(0, (date_today - start_date).days + 1)
            if (start_date + timedelta(days=i)).isoweekday() <= NB_BUSINESS_DAYS
        ] + non_terminated_downloads
        todo_dates.sort()
        return todo_dates

    def get_periods(self, dates: List[str], limit: date) -> List[Tuple[date, date]]:
        if not dates:
            return []
        start_date = datetime.strptime(dates[0], DATE_DIR_FORMAT).date()
        end_date = datetime.strptime(dates[-1], DATE_DIR_FORMAT).date()
        max_date = min(limit, end_date)
        periods: List[Tuple[date, date]] = [
            (
                start_date + timedelta(days=i),
                min(
                    start_date
                    + timedelta(days=i - 1)
                    + self.api_access.MAX_DOWNLOAD_FRAME,
                    max_date,
                ),
            )
            for i in range(
                0, (limit - start_date).days, self.api_access.MAX_DOWNLOAD_FRAME.days,
            )
        ]
        return periods

    def get_tickers_list(self, date_today: date) -> List[str]:
        tickers = self.api_access.get_tickers()
        tickers_df = pd.DataFrame()
        tickers_df["tickers"] = tickers
        self.storage.create_directory(DATASETS_DIR, TICKERS_DIR)
        self.storage.write(
            "{}/{}/{}_{}.csv".format(
                DATASETS_DIR,
                TICKERS_DIR,
                TICKERS_FILE_BASE_NAME,
                date_today.strftime(DATE_DIR_FORMAT),
            ),
            tickers_df.to_csv(index=False),
        )
        return tickers

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
            if self.storage.exists(date_dir_path):
                self.storage.write(termination_file_path, "")
                for none_ticker in none_tickers:
                    self.storage.write(
                        "{}/{}_{}.txt".format(none_dir, none_ticker, date_str), ""
                    )
                for error in errors:
                    ticker, error_message = error
                    self.storage.write(
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

    def get_tickers_for_all_periods(
        self, periods: List[Tuple[date, date]], tickers: List[str], date_today: date
    ) -> None:
        for period in periods:
            none_tickers: List[str] = []
            errors: List[Tuple[str, str]] = []
            self.get_all_tickers_data(tickers, period, none_tickers, errors)

            # handle_states
            period_dates = self.get_period_dates(period)
            period_dates = [
                period_date for period_date in period_dates if period_date <= date_today
            ]
            self.handle_states(period_dates, none_tickers, errors)

    def start_flow(self) -> None:
        date_today: date = date.today()

        # get remaining dates for downloading data
        todo_dates = self.get_todo_dates(date_today)
        if not todo_dates:
            return
        LOG.info("Todo dates: {}".format(todo_dates))

        # prepare storage and create all needed directories
        self.storage.create_all_dates_directories(DATASETS_DIR, todo_dates)

        # generate periods for downloading depending on the api download frame
        periods: List[Tuple[date, date]] = self.get_periods(todo_dates, date_today)
        LOG.info("Periods: {}".format(periods))

        # get tickers
        tickers = self.get_tickers_list(date_today)
        # tickers = ["aapl", "amzn", "googl", "fb"]

        # get all tickers data for all periods
        self.get_tickers_for_all_periods(periods, tickers, date_today)
