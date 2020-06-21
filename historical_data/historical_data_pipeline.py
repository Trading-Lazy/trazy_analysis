import asyncio
import logging
import pandas as pd
import pytz
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from historical_data.common import (
    concat_path,
    DATE_DIR_FORMAT,
    DONE_DIR,
    ENCODING,
    ERROR_DIR,
    LOG,
    NONE_DIR,
    STATUS_CODE_OK,
    TICKERS_DIR,
)
from historical_data.historical_data_api_access import HistoricalDataApiAccess
from historical_data.storage import Storage
from typing import Dict, List, Tuple

NB_BUSINESS_DAYS = 5
NB_WEEK_DAYS = 7
TERMINATION_FILE = "terminated.txt"
TICKERS_FILE_BASE_NAME = "tickers_all"
LOGS_DIR = "logs"


class HistoricalDataPipeline:
    def __init__(
        self,
        api_access: HistoricalDataApiAccess,
        storage: Storage,
        historical_data_output="datasets",
    ):
        self.api_access = api_access
        self.storage = storage
        self.output = historical_data_output

    def process_ticker_data(self, ticker: str, data: str) -> Dict[str, str]:
        groups_df = self.api_access.parse_ticker_data(data)
        for date_str, group_df in groups_df:
            self.storage.create_date_directories(self.output, date_str)
            path = concat_path(self.output, date_str)
            self.storage.write(
                "{}/{}/{}_{}.csv".format(path, DONE_DIR, ticker, date_str),
                group_df.to_csv(index=False),
            )

    def get_ticker_csv(
        self,
        ticker: str,
        period: Tuple[date, date],
        none_tickers: List[str],
        errors: List[Tuple[str, str]],
    ) -> None:
        url_ticker = self.api_access.generate_url(ticker, period)
        with requests.Session() as session:
            response = session.get(url_ticker)

            if response.status_code == STATUS_CODE_OK:
                data: str = response.content.decode(ENCODING)
                if data:
                    self.process_ticker_data(ticker, data)
                else:
                    none_tickers.append(ticker)
            else:
                errors.append((ticker, str(response.status_code)))

    async def get_tickers_asynchronous(
        self, tickers, period, none_tickers: List[str], errors: List[Tuple[str, str]]
    ):
        with ThreadPoolExecutor(max_workers=8) as executor:
            loop = asyncio.get_event_loop()
            for ticker in tickers:
                loop.run_in_executor(
                    executor,
                    self.get_ticker_csv,
                    *(ticker, period, none_tickers, errors)
                )
                await asyncio.sleep(0.01)

    def get_todo_dates(self, date_today) -> List[str]:
        dirs_list = self.storage.ls(self.output)
        dates = [name for name in dirs_list if not name.startswith(".")]
        start_date = self.api_access.earliest_available_date_for_download
        start_date_str = start_date.strftime(DATE_DIR_FORMAT)
        avalaible_dates_for_download = [
            date for date in dates if date >= start_date_str
        ]
        LOG.info(
            "Avalaible dates for download: {}".format(avalaible_dates_for_download)
        )
        non_terminated_downloads = [
            date_dir[0:8]
            for date_dir in avalaible_dates_for_download
            if TERMINATION_FILE
            not in self.storage.ls("{}/{}".format(self.output, date_dir))
        ]
        LOG.info("Non terminated downloads: {}".format(non_terminated_downloads))
        start_date = self.api_access.earliest_available_date_for_download
        try:
            last_date = datetime.strptime(max(dates)[0:8], DATE_DIR_FORMAT).date()
            start_date = last_date + timedelta(days=1)
        except:
            pass
        if start_date.isoweekday() > NB_BUSINESS_DAYS:
            start_date += timedelta(days=(NB_WEEK_DAYS + 1 - start_date.isoweekday()))
        todo_dates = [
            (start_date + timedelta(days=i)).strftime(DATE_DIR_FORMAT)
            for i in range(0, (date_today - start_date).days)
            if (start_date + timedelta(days=i)).isoweekday() <= NB_BUSINESS_DAYS
        ] + non_terminated_downloads
        todo_dates.sort()
        return todo_dates

    def get_periods(self, dates: List[str]) -> List[Tuple[date, date]]:
        start_date = datetime.strptime(dates[0], DATE_DIR_FORMAT).date()
        date_today = datetime.today().date()
        periods: List[Tuple[date, date]] = [
            (
                start_date + timedelta(days=i),
                start_date + timedelta(days=i - 1) + self.api_access.max_download_frame,
            )
            for i in range(
                0,
                (date_today - start_date).days,
                self.api_access.max_download_frame.days,
            )
        ]
        return periods

    def get_tickers_list(self, date_today: date) -> List[str]:
        tickers = self.api_access.get_tickers()
        tickers_df = pd.DataFrame()
        tickers_df["tickers"] = tickers
        self.storage.create_directory(self.output, TICKERS_DIR)
        self.storage.write(
            "{}/{}/{}_{}.csv".format(
                self.output,
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
            date_dir_path = "{}/{}".format(self.output, date_str)
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
                    ticker, status_code = error
                    self.storage.write(
                        "{}/{}_{}.txt".format(error_dir, ticker, date_str), status_code,
                    )

    def get_period_dates(self, period: Tuple[date, date], date_today: date):
        period_start_date = period[0]
        period_end_date = min(date_today, period[1])
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
            loop = asyncio.get_event_loop()
            loop.run_until_complete(
                self.get_tickers_asynchronous(tickers, period, none_tickers, errors)
            )

            # handle_states
            period_dates = self.get_period_dates(period, date_today)
            self.handle_states(period_dates, none_tickers, errors)

    def start_flow(self) -> None:
        date_today = datetime.today().date()

        # get remaining dates for downloading data
        todo_dates = self.get_todo_dates(date_today)
        if not todo_dates:
            return
        LOG.info("Todo dates: {}".format(todo_dates))

        # prepare storage and create all needed directories
        self.storage.create_all_dates_directories(self.output, todo_dates)

        # generate periods for downloading depending on the api download frame
        periods: List[Tuple[date, date]] = self.get_periods(todo_dates)
        LOG.info("Periods: {}".format(periods))

        # get tickers
        tickers = self.get_tickers_list(date_today)
        tickers = ["aapl"]

        # get all tickers data for all periods
        self.get_tickers_for_all_periods(periods, tickers, date_today)
