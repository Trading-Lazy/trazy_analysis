import os
import traceback
from typing import List

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.constants import CONNECTION_ERROR_MESSAGE
from trazy_analysis.market_data.ccxt_data_handler import CcxtDataHandler
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class CcxtLiveDataHandler(CcxtDataHandler):
    def __init__(self, ccxt_connector: CcxtConnector):
        super().__init__(ccxt_connector)

    def request_ticker_lastest_candle(self, ticker: Asset) -> Candle:
        latest_candles = self.request_ticker_lastest_candles(ticker)
        if len(latest_candles) == 0:
            return None
        return latest_candles[-1]

    def request_ticker_lastest_candles(
        self, ticker: Asset, nb_candles: int = 1
    ) -> List[Candle]:
        exchange_to_lower = ticker.exchange.lower()
        exchange_instance = self.ccxt_connector.get_exchange_instance(exchange_to_lower)
        try:
            raw_candles = exchange_instance.fetchOHLCV(symbol=ticker.symbol)
            candle_dataframe = self.ticker_data_to_dataframe(ticker, raw_candles)
            latest_candles = candle_dataframe.to_candles()
            start = max(-nb_candles, -len(latest_candles))
            return latest_candles[start:]
        except Exception as e:
            LOG.error(
                CONNECTION_ERROR_MESSAGE,
                str(e),
                traceback.format_exc(),
            )
            return []
