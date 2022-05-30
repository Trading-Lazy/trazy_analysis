from collections import deque
from datetime import datetime
from typing import List, Dict, Any

import ccxt
import numpy as np
import pandas as pd
import pytz
from pandas_market_calendars import MarketCalendar

from trazy_analysis.bot.event_loop import EventLoop
from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.broker.simulated_broker import SimulatedBroker
from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.clock import SimulatedClock
from trazy_analysis.common.constants import NONE_API_KEYS
from trazy_analysis.common.crypto_exchange_calendar import CryptoExchangeCalendar
from trazy_analysis.db_storage.db_storage import DbStorage
from trazy_analysis.feed.feed import (
    Feed,
    CsvFeed,
    HistoricalFeed,
    PandasFeed,
    ExternalStorageFeed,
)
from trazy_analysis.file_storage.file_storage import FileStorage
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.market_data.historical.ccxt_historical_data_handler import (
    CcxtHistoricalDataHandler,
)
from trazy_analysis.market_data.historical.historical_data_handler import (
    HistoricalDataHandler,
)
from trazy_analysis.market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import OrderType
from trazy_analysis.order_manager.order_creator import OrderCreator
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.order_manager.position_sizer import PositionSizer
from trazy_analysis.statistics.statistics import Statistics


class Backtest:
    def __init__(
        self,
        assets: List[Asset],
        strategies_parameters: Dict[type, List[Dict[str, Any]]],
        fee_model,
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
        initial_funds: float = 10000.0,
        market_cal: MarketCalendar = CryptoExchangeCalendar(),
        integer_size: bool = False,
        fixed_order_type: OrderType = OrderType.MARKET,
        limit_order_pct=0.005,
        stop_order_pct=0.05,
        target_order_pct=0.01,
        trailing_stop_order_pct=0.05,
        with_cover=False,
        with_bracket=False,
        with_trailing_cover=False,
        with_trailing_bracket=False,
        csv_filenames: Dict[Asset, str] = None,
        csv_file_sep: str = None,
        download: bool = False,
        exchanges_api_keys: Dict[str, str] = {},
        candle_dataframes: np.array = None,
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        save_db_storage: DbStorage = None,
        save_csv_filenames: Dict[Asset, str] = None,
        preload: bool = True,
        close_at_end_of_day=True,
        close_at_end_of_data=True,
        statistics_class: type = Statistics,
    ):
        events = deque()
        if csv_filenames is not None:
            kwargs = {"sep": csv_file_sep} if csv_file_sep is not None else {}
            feed = CsvFeed(csv_filenames=csv_filenames, events=events, **kwargs)
        elif download:
            exchanges = [asset.exchange.lower() for asset in assets]
            historical_data_handlers = {}
            exchanges_api_keys = {
                exchange.lower(): api_key
                for exchange, api_key in exchanges_api_keys.items()
            }


            # Crypto currency exchanges
            ccxt_exchanges_api_keys = {}
            ccxt_exchanges = ccxt.exchanges
            other_exchanges = []
            for exchange in exchanges:
                if exchange in ccxt_exchanges:
                    if exchange in exchanges_api_keys:
                        ccxt_exchanges_api_keys[exchange] = exchanges_api_keys[exchange]
                    else:
                        ccxt_exchanges_api_keys[exchange] = NONE_API_KEYS
                else:
                    other_exchanges.append(exchange)
            if len(ccxt_exchanges_api_keys) != 0:
                ccxt_connector = CcxtConnector(
                    exchanges_api_keys=ccxt_exchanges_api_keys
                )
                ccxt_historical_data_handler = CcxtHistoricalDataHandler(ccxt_connector)
                for exchange in ccxt_exchanges_api_keys:
                    historical_data_handlers[exchange] = ccxt_historical_data_handler

            # Other exchanges
            for exchange in other_exchanges:
                if exchange == "iex":
                    historical_data_handlers["iex"] = TiingoHistoricalDataHandler()
                else:
                    all_exchanges = ccxt_exchanges
                    all_exchanges.append("iex")
                    raise Exception(
                        f"Exchange {exchange} is not supported for now. The list of supported exchanges is: {all_exchanges}"
                    )

            feed = HistoricalFeed(
                assets=assets,
                historical_data_handlers=historical_data_handlers,
                start=start,
                end=end,
                events=events,
            )
        elif candle_dataframes is not None:
            feed = PandasFeed(candle_dataframes=candle_dataframes, events=events)
        elif db_storage is not None:
            feed = ExternalStorageFeed(
                assets=assets,
                start=start,
                end=end,
                events=events,
                db_storage=db_storage,
                market_cal=market_cal,
            )
        elif file_storage is not None:
            feed = ExternalStorageFeed(
                assets=assets,
                start=start,
                end=end,
                events=events,
                file_storage=file_storage,
                market_cal=market_cal,
            )
        else:
            raise Exception(
                "No source for data feed, one of "
                "[csv_filenames, candle_dataframes, db_storage, file_storage]"
                " should not be None, or download should not be False"
            )

        # Save the feeds for reuse
        if save_db_storage is not None:
            for _, candle_dataframe in feed.candle_dataframes.items():
                save_db_storage.add_candle_dataframe(candle_dataframe)
        elif save_csv_filenames is not None:
            for asset, candle_dataframe in feed.candle_dataframes.items():
                candle_dataframe.to_csv(save_csv_filenames[asset], csv_file_sep)

        self.events = feed.events
        self.clock = SimulatedClock(market_cal=market_cal)
        self.exchanges = [asset.exchange for asset in assets]
        brokers = {}
        for exchange in self.exchanges:
            brokers[exchange] = SimulatedBroker(
                self.clock,
                self.events,
                initial_funds=initial_funds,
                fee_model=fee_model,
            )
            brokers[exchange].subscribe_funds_to_portfolio(initial_funds)
        broker_manager = BrokerManager(brokers=brokers, clock=self.clock)
        position_sizer = PositionSizer(
            broker_manager=broker_manager, integer_size=integer_size
        )
        order_creator = OrderCreator(
            broker_manager=broker_manager,
            fixed_order_type=fixed_order_type,
            limit_order_pct=limit_order_pct,
            stop_order_pct=stop_order_pct,
            target_order_pct=target_order_pct,
            trailing_stop_order_pct=trailing_stop_order_pct,
            with_cover=with_cover,
            with_bracket=with_bracket,
            with_trailing_cover=with_trailing_cover,
            with_trailing_bracket=with_trailing_bracket,
        )
        order_manager = OrderManager(
            events=self.events,
            broker_manager=broker_manager,
            position_sizer=position_sizer,
            order_creator=order_creator,
        )
        indicators_manager = IndicatorsManager(
            preload=preload, initial_data=feed.candles
        )
        self.event_loop = EventLoop(
            events=self.events,
            assets=assets,
            feed=feed,
            order_manager=order_manager,
            strategies_parameters=strategies_parameters,
            indicators_manager=indicators_manager,
            close_at_end_of_day=close_at_end_of_day,
            close_at_end_of_data=close_at_end_of_data,
            statistics_class=statistics_class,
        )

    def run(self) -> None:
        self.events = deque()
        self.event_loop.loop()

    def statistics_df(self) -> pd.DataFrame:
        return self.event_loop.statistics_df
