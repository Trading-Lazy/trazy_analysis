from unittest.mock import patch

from historical_data.historical_data_feed import historical_data_feed


@patch("historical_data.historical_data_pipeline.HistoricalDataPipeline.start_flow")
def test_historical_data_feed(start_flow_mocked):
    historical_data_feed()
    start_flow_mocked.assert_has_calls([])
