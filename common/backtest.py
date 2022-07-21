from collections import deque
from datetime import datetime, timedelta
from typing import List, Dict, Any, Union, Optional

import ccxt
import numpy as np
import pandas as pd
import pytz
from pandas_market_calendars import MarketCalendar

from trazy_analysis.bot.event_loop import EventLoop
from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.broker.fee_model import FeeModel, FeeModelManager
from trazy_analysis.broker.simulated_broker import SimulatedBroker
from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.clock import SimulatedClock
from trazy_analysis.common.constants import NONE_API_KEYS
from trazy_analysis.common.crypto_exchange_calendar import CryptoExchangeCalendar
from trazy_analysis.db_storage.db_storage import DbStorage
from trazy_analysis.feed.feed import (
    CsvFeed,
    HistoricalFeed,
    PandasFeed,
    ExternalStorageFeed,
)
from trazy_analysis.file_storage.file_storage import FileStorage
from trazy_analysis.market_data.data_fetcher import AssetDataFetcher
from trazy_analysis.market_data.historical.ccxt_historical_data_handler import (
    CcxtHistoricalDataHandler,
)
from trazy_analysis.market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import (
    OrderType,
    BrokerIsolation,
    EventLoopMode,
    IndicatorMode,
)
from trazy_analysis.order_manager.order_creator import OrderCreator
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.order_manager.position_sizer import PositionSizer
from trazy_analysis.statistics.statistics import Statistics


class BacktestConfig:
    def __init__(
        self,
        assets: dict[Asset, timedelta | list[timedelta]],
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
        fee_models: Optional[FeeModel | dict[Asset, FeeModel]] = None,
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
        csv_filenames: Optional[dict[Asset, dict[timedelta, str]]] = None,
        csv_file_sep: str = None,
        exchanges_api_keys: dict[str, str] = None,
        candle_dataframes: np.array = None,
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        store_medium: Optional[
            dict[Asset, dict[timedelta, str]] | DbStorage | FileStorage
        ] = None,
        indicator_mode: IndicatorMode = IndicatorMode.LIVE,
        close_at_end_of_day=True,
        close_at_end_of_data=True,
        isolation: BrokerIsolation = BrokerIsolation.EXCHANGE,
        statistics_class: type = Statistics,
        events: deque = deque(),
    ):
        self.assets = assets
        self.fee_models = fee_models
        self.fee_models_manager = FeeModelManager(self.fee_models)
        self.start = start
        self.end = end
        self.initial_funds = initial_funds
        self.market_cal = market_cal
        self.integer_size = integer_size
        self.fixed_order_type = fixed_order_type
        self.limit_order_pct = limit_order_pct
        self.stop_order_pct = stop_order_pct
        self.target_order_pct = target_order_pct
        self.trailing_stop_order_pct = trailing_stop_order_pct
        self.with_cover = with_cover
        self.with_bracket = with_bracket
        self.with_trailing_cover = with_trailing_cover
        self.with_trailing_bracket = with_trailing_bracket
        self.csv_filenames = csv_filenames
        self.csv_file_sep = csv_file_sep
        self.exchanges_api_keys = (
            exchanges_api_keys if exchanges_api_keys is not None else {}
        )
        self.candle_dataframes = candle_dataframes
        self.db_storage = db_storage
        self.file_storage = file_storage
        self.store_medium = store_medium
        self.indicator_mode = indicator_mode
        self.close_at_end_of_day = close_at_end_of_day
        self.close_at_end_of_data = close_at_end_of_data
        self.isolation = isolation
        self.statistics_class = statistics_class

        self.events = events
        self.feed = None
        if csv_filenames is not None:
            kwargs = {"sep": csv_file_sep} if csv_file_sep is not None else {}
            self.feed = CsvFeed(
                csv_filenames=csv_filenames, events=self.events, **kwargs
            )
        elif candle_dataframes is not None:
            self.feed = PandasFeed(
                candle_dataframes=candle_dataframes, events=self.events
            )
        elif db_storage is not None:
            self.feed = ExternalStorageFeed(
                assets=assets,
                start=start,
                end=end,
                events=self.events,
                db_storage=db_storage,
                market_cal=market_cal,
            )
        elif file_storage is not None:
            self.feed = ExternalStorageFeed(
                assets=assets,
                start=start,
                end=end,
                events=self.events,
                file_storage=file_storage,
                market_cal=market_cal,
            )
        elif assets is not None:
            self.feed, fee_models = AssetDataFetcher.fetch(assets, start, end, exchanges_api_keys)
            self.fee_models_manager = FeeModelManager(fee_models)
        else:
            raise Exception(
                "No source for data feed, one of "
                "[csv_filenames, candle_dataframes, db_storage, file_storage]"
                " should not be None, or download should not be False"
            )

        # Save the feeds for reuse
        if isinstance(store_medium, dict):
            for (
                asset,
                time_unit_candle_dataframes,
            ) in self.feed.candle_dataframes.items():
                for time_unit, candle_dataframe in time_unit_candle_dataframes.items():
                    candle_dataframe.to_csv(
                        store_medium[asset][time_unit], csv_file_sep
                    )
        elif isinstance(store_medium, DbStorage):
            for _, time_unit_candle_dataframes in self.feed.candle_dataframes.items():
                for _, candle_dataframe in time_unit_candle_dataframes.items():
                    store_medium.add_candle_dataframe(candle_dataframe)
        # TODO handle file_storage case
        # elif isinstance(store_medium, FileStorage):
        #     for _, time_unit_candle_dataframes in self.feed.candle_dataframes.items():
        #         for _, candle_dataframe in time_unit_candle_dataframes:
        #             store_medium.write(candle_dataframe)


class Backtest:
    def __init__(
        self,
        assets: dict[Asset, timedelta | list[timedelta]],
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
        fee_models: Optional[FeeModel | dict[Asset, FeeModel]] = None,
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
        csv_filenames: dict[Asset, str] = None,
        csv_file_sep: str = None,
        exchanges_api_keys: dict[str, str] = {},
        candle_dataframes: np.array = None,
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        store_medium: Optional[
            dict[Asset, dict[timedelta, str]] | DbStorage | FileStorage
        ] = None,
        indicator_mode: IndicatorMode = IndicatorMode.LIVE,
        close_at_end_of_day=True,
        close_at_end_of_data=True,
        isolation: BrokerIsolation = BrokerIsolation.EXCHANGE,
        statistics_class: type = Statistics,
        backtest_config: BacktestConfig = None,
    ):
        self.events = deque()
        if backtest_config is None:
            backtest_config = BacktestConfig(
                assets=assets,
                fee_models=fee_models,
                start=start,
                end=end,
                initial_funds=initial_funds,
                market_cal=market_cal,
                integer_size=integer_size,
                fixed_order_type=fixed_order_type,
                limit_order_pct=limit_order_pct,
                stop_order_pct=stop_order_pct,
                target_order_pct=target_order_pct,
                trailing_stop_order_pct=trailing_stop_order_pct,
                with_cover=with_cover,
                with_bracket=with_bracket,
                with_trailing_cover=with_trailing_cover,
                with_trailing_bracket=with_trailing_bracket,
                csv_filenames=csv_filenames,
                csv_file_sep=csv_file_sep,
                exchanges_api_keys=exchanges_api_keys,
                candle_dataframes=candle_dataframes,
                db_storage=db_storage,
                file_storage=file_storage,
                store_medium=store_medium,
                indicator_mode=indicator_mode,
                close_at_end_of_day=close_at_end_of_day,
                close_at_end_of_data=close_at_end_of_data,
                isolation=isolation,
                statistics_class=statistics_class,
                events=self.events,
            )
        else:
            backtest_config.events = self.events

        self.backtest_config = backtest_config
        self.exchanges = [asset.exchange for asset in self.backtest_config.assets]
        self.event_loop: Optional[EventLoop] = None

    def run_strategies(
        self,
        strategies_parameters: dict[type, dict[str, Any]],
    ) -> pd.DataFrame:
        self.events = deque()
        self.backtest_config.feed.events = self.events
        self.backtest_config.feed.reset()
        clock = SimulatedClock(market_cal=self.backtest_config.market_cal)
        brokers = {}
        for exchange in self.exchanges:
            brokers[exchange] = SimulatedBroker(
                clock,
                self.events,
                initial_funds=self.backtest_config.initial_funds,
                fee_models=self.backtest_config.fee_models,
            )
            brokers[exchange].subscribe_funds_to_portfolio(
                self.backtest_config.initial_funds
            )
        broker_manager = BrokerManager(brokers=brokers)
        position_sizer = PositionSizer(
            broker_manager=broker_manager,
            integer_size=self.backtest_config.integer_size,
        )
        order_creator = OrderCreator(
            broker_manager=broker_manager,
            fixed_order_type=self.backtest_config.fixed_order_type,
            limit_order_pct=self.backtest_config.limit_order_pct,
            stop_order_pct=self.backtest_config.stop_order_pct,
            target_order_pct=self.backtest_config.target_order_pct,
            trailing_stop_order_pct=self.backtest_config.trailing_stop_order_pct,
            with_cover=self.backtest_config.with_cover,
            with_bracket=self.backtest_config.with_bracket,
            with_trailing_cover=self.backtest_config.with_trailing_cover,
            with_trailing_bracket=self.backtest_config.with_trailing_bracket,
        )
        order_manager = OrderManager(
            events=self.events,
            broker_manager=broker_manager,
            position_sizer=position_sizer,
            order_creator=order_creator,
            clock=clock,
        )
        self.event_loop = EventLoop(
            events=self.events,
            assets=self.backtest_config.assets,
            feed=self.backtest_config.feed,
            order_manager=order_manager,
            strategies_parameters=strategies_parameters,
            indicator_mode=self.backtest_config.indicator_mode,
            mode=EventLoopMode.BATCH,
            close_at_end_of_day=self.backtest_config.close_at_end_of_day,
            close_at_end_of_data=self.backtest_config.close_at_end_of_data,
            broker_isolation=self.backtest_config.isolation,
            statistics_class=self.backtest_config.statistics_class,
            real_time_plotting=False,
        )
        self.event_loop.loop()

    def get_statistics(self) -> pd.DataFrame:
        df = self.event_loop.statistics_df
        if df.empty:
            data = {"index": ["Sortino Ratio"], "Backtest results": [-1]}
            df = pd.DataFrame(data).set_index("index")
        return df

    def plot(self, asset: Asset, time_unit: timedelta) -> None:
        self.event_loop.plot(asset, time_unit)

    def plot_indicators_instances_graph(self):
        self.event_loop.plot_indicators_instances_graph()

    def run_strategy(
        self, strategy: type, strategy_parameters: dict[str, Any]
    ) -> pd.DataFrame:
        self.run_strategies({strategy: strategy_parameters})
