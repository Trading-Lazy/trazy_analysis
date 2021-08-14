from trazy_analysis.file_storage.file_storage import FileStorage
from trazy_analysis.file_storage.meganz_file_storage import MegaNzFileStorage
from trazy_analysis.market_data.historical.historical_data_handler import (
    HistoricalDataHandler,
)
from trazy_analysis.market_data.historical.historical_data_pipeline import (
    HistoricalDataPipeline,
)
from trazy_analysis.market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)


def start_historical_data_pipeline() -> None:
    file_storage: FileStorage = MegaNzFileStorage()
    historical_data_handler: HistoricalDataHandler = TiingoHistoricalDataHandler()
    pipeline: HistoricalDataPipeline = HistoricalDataPipeline(
        historical_data_handler, file_storage
    )
    pipeline.start_flow()
