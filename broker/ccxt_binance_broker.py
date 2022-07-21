import os
from collections import deque
from datetime import timedelta
from typing import Any, Dict, List, Tuple

import pandas as pd

import trazy_analysis.settings
from trazy_analysis.broker.ccxt_broker import CcxtBroker
from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.clock import Clock
from trazy_analysis.logger import logger

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class CcxtBinanceBroker(CcxtBroker):
    # Every period the positions of the broker get updated
    UPDATE_PRICE_PERIOD = pd.Timedelta(value=10, unit="seconds")
    UPDATE_LOT_SIZE_INFO = timedelta(days=1)
    UPDATE_BALANCES_PERIOD = pd.Timedelta(value=10, unit="seconds")
    UPDATE_TRANSACTIONS_PERIOD = pd.Timedelta(value=10, unit="seconds")
    PRODUCT_INFO_PERIOD = pd.Timedelta(value=10, unit="days")
    SYMBOL_INFO_PERIOD = pd.Timedelta(value=10, unit="days")
    TRANSACTION_LOOKBACK_PERIOD = timedelta(minutes=10)
    CURRENCY_MAPPING = {"EUR": "EUR", "USD": "USD"}

    def __init__(
        self,
        clock: Clock,
        events: deque,
        ccxt_connector: CcxtConnector,
        base_currency: str = "EUR",
        supported_currencies: list[str] = ["EUR", "USDT"],
        execute_at_end_of_day=True,
    ):
        super().__init__(
            clock=clock,
            events=events,
            ccxt_connector=ccxt_connector,
            base_currency=base_currency,
            supported_currencies=supported_currencies,
            execute_at_end_of_day=execute_at_end_of_day,
        )

    def parse_lot_size_info(self, symbol_info: Any) -> Tuple[str, float]:
        symbol = symbol_info["symbol"]
        symbol_filters = symbol_info["filters"]
        for symbol_filter in symbol_filters:
            if symbol_filter["filterType"] != "LOT_SIZE":
                continue
            symbol_lot_size = float(symbol_filter["minQty"])
            break
        return symbol, symbol_lot_size

    def parse_price_info(self, price_info: Any) -> float:
        price = float(price_info["last"])
        return price

    def parse_balances_info(self, balance_info: Any) -> dict[str, float]:
        raise NotImplementedError("Should implement parse_balances_info()")

    def parse_trade_info(self, trade_info: Any) -> dict[str, float]:
        raise NotImplementedError("Should implement parse_trade_info()")
