from unittest.mock import patch

from market_data.historical.start_historical_data_pipeline import (
    start_historical_data_pipeline,
)


@patch(
    "market_data.historical.historical_data_pipeline.HistoricalDataPipeline.start_flow"
)
def test_historical_data_feed(start_flow_mocked):
    start_historical_data_pipeline()
    start_flow_mocked.assert_has_calls([])
