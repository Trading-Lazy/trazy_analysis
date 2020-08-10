import abc
from typing import List

from historical_data.common import DONE_DIR, ERROR_DIR, NONE_DIR, concat_path


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

    @abc.abstractmethod
    def get_file_content(self, path: str) -> str:
        raise NotImplementedError

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
