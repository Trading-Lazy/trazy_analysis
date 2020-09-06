from file_storage.file_storage import FileStorage
from file_storage.meganz_file_storage import MegaNzFileStorage
from market_data.historical.historical_data_handler import HistoricalDataHandler
from market_data.historical.historical_data_pipeline import HistoricalDataPipeline
from market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)


def start_historical_data_pipeline() -> None:
    file_storage: FileStorage = MegaNzFileStorage()
    historical_data_handler: HistoricalDataHandler = TiingoHistoricalDataHandler()
    pipeline: HistoricalDataPipeline = HistoricalDataPipeline(
        historical_data_handler, file_storage
    )
    pipeline.start_flow()
