from historical_data.historical_data_api_access import HistoricalDataApiAccess
from historical_data.historical_data_pipeline import HistoricalDataPipeline
from historical_data.meganz_storage import MegaNzStorage
from historical_data.storage import Storage
from historical_data.tiingo_api_access import TiingoApiAccess

if __name__ == "__main__":
    storage: Storage = MegaNzStorage()
    api_access: HistoricalDataApiAccess = TiingoApiAccess()
    pipeline: HistoricalDataPipeline = HistoricalDataPipeline(api_access, storage)
    pipeline.start_flow()
