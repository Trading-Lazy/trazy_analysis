from pathlib import Path

NONE_DIR = "NONE"
ERROR_DIR = "ERROR"
DONE_DIR = "DONE"
DATASETS_DIR = "datasets"
TICKERS_DIR = "tickers"
PATH_SEPARATOR = "/"


def concat_path(base_path: str, complement: str) -> str:
    if not base_path or not complement:
        return base_path + complement

    path = Path(base_path) / complement
    return str(path)
