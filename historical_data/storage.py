import abc
import asyncio
from historical_data.common import (
    concat_path,
    NONE_DIR,
    ERROR_DIR,
    DONE_DIR,
    TICKERS_DIR,
)
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple
from logging import StreamHandler


class Storage:
    @abc.abstractmethod
    def exists(self, path: str) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def ls(self, path: str) -> List[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def mkdir(self, path: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def write(self, path: str, content: str) -> None:
        raise NotImplementedError

    def get_log_filename(self, identifer: str) -> str:
        return "log_{}.log".format(identifer)

    def async_write(self, path: str) -> None:
        with ThreadPoolExecutor(max_workers=8) as executor:
            loop = asyncio.get_event_loop()
            loop.run_in_executor(executor, self.write, *(path))

    def bulk_write(
        self, filename_content_tuples: List[Tuple[str, str]], asynchronous: bool = False
    ) -> None:
        write = self.async_write if asynchronous else self.write
        for filename_content_tuple in filename_content_tuples:
            filename, content = filename_content_tuple
            write(filename, content)

    def create_date_directories(self, base_path: str, date_str: str) -> None:
        path = concat_path(base_path, date_str)
        self.mkdir(path)
        self.mkdir("{}/{}/".format(path, NONE_DIR))
        self.mkdir("{}/{}/".format(path, ERROR_DIR))
        self.mkdir("{}/{}/".format(path, DONE_DIR))

    def create_all_dates_directories(self, base_path, dates_strs) -> None:
        for date_str in dates_strs:
            self.create_date_directories(base_path, date_str)

    def create_directory(self, dest_path: str, directory: str) -> None:
        path = concat_path(dest_path, directory)
        self.mkdir(path)
