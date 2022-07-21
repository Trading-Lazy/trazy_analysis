import os
from collections import deque
from datetime import timedelta
from threading import Thread
from typing import Any, Dict, List, Union, Tuple

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.common.constants import MAX_TIMESTAMP
from trazy_analysis.common.helper import get_or_create_nested_dict, normalize_assets
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.feed.feed import Feed
from trazy_analysis.indicators.indicator import CandleData
from trazy_analysis.indicators.indicators_managers import ReactiveIndicators
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import (
    Action,
    Direction,
    EventType,
    BrokerIsolation,
    StrategyParametersIsolation,
    IndicatorMode,
    EventLoopMode,
)
from trazy_analysis.models.event import (
    AssetSpecificEvent,
    DataEvent,
    MarketEodDataEvent,
    PendingSignalEvent,
)
from trazy_analysis.models.order import Order
from trazy_analysis.models.signal import SignalBase, Signal, MultipleSignal
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.position.transaction import Transaction
from trazy_analysis.statistics.statistics_manager import StatisticsManager
from trazy_analysis.strategy.context import Context
from trazy_analysis.strategy.strategy import StrategyBase, Strategy, MultiAssetsStrategy

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


# It's a thread that propagates exceptions to the main thread
class PropagatingThread(Thread):
    def run(self):
        self.exc = None
        try:
            if hasattr(self, "_Thread__target"):
                # Thread uses name mangling prior to Python 3.
                self.ret = self._Thread__target(
                    *self._Thread__args, **self._Thread__kwargs
                )
            else:
                self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exc = e

    def join(self):
        super(PropagatingThread, self).join()
        if self.exc:
            raise self.exc
        return self.ret


# It's a set that expires its elements after a certain amount of time
class ExpiringSet:
    def __init__(self, max_len: int = 10):
        self.max_len = max_len
        self.set = set()
        self.ordered_elts = deque()

    def add(self, item: Any):
        if item in self.set:
            return
        self.set.add(item)
        self.ordered_elts.append(item)
        while len(self.ordered_elts) > self.max_len:
            elt_to_discard = self.ordered_elts.popleft()
            self.set.discard(elt_to_discard)

    def __contains__(self, item: Any):
        return item in self.set


# EventLoop is a class that runs a loop that processes events.
class EventLoop:
    def _init_strategy_instance(
        self, strategy_class: type, parameters: dict[str, float]
    ):
        """
        > This function initializes the strategy instances, which are the objects that will be used to generate signals

        :param strategy_class: The strategy class to be instantiated
        :type strategy_class: type
        :param parameters: dict[str, float]
        :type parameters: dict[str, float]
        """
        if issubclass(strategy_class, Strategy):
            for asset in self.assets:
                for time_unit in self.assets[asset]:
                    if self.data.exists(asset, time_unit):
                        strategy = strategy_class(
                            self.data(asset, time_unit),
                            parameters,
                            self.indicators,
                        )
                        strategy.set_context(self.context)
                        self.strategy_instances.append(strategy)
        elif issubclass(strategy_class, MultiAssetsStrategy):
            strategy = strategy_class(self.data, parameters, self.indicators)
            strategy.set_context(self.context)
            self.strategy_instances.append(strategy)

    def _init_strategy_instances(self):
        """
        It creates a new instance of each strategy class
        """
        for strategy_class in self.strategies_parameters:
            parameters = self.strategies_parameters[strategy_class]
            self._init_strategy_instance(strategy_class, parameters)

    def _init_seen_candles(self):
        """
        It creates a dictionary of dictionaries of ExpiringSets
        """
        for asset in self.assets:
            get_or_create_nested_dict(self.seen_candles, asset)
            for time_unit in self.assets[asset]:
                self.seen_candles[asset][time_unit] = ExpiringSet()

    def run_strategy(self, strategy: StrategyBase):
        """
        > The function `run_strategy` takes a strategy object and calls the `process_context` function on it, passing in the
        context and clock objects

        :param strategy: The strategy object to be run
        :type strategy: StrategyBase
        """
        strategy.process_context(self.context, self.clock)

    def update_equity_curves(self):
        """
        If the current time is greater than the most recent time in the equity curve, then add the current time and the
        current equity to the equity curve
        :return: The equity curves for each exchange.
        """
        exchanges = {asset.exchange for asset in self.assets}
        for exchange in exchanges:
            if self.statistics_manager.get_equity_curves(exchange) is None:
                self.statistics_manager.set_equity_curves(deque(), exchange)

            if not self.clock.updated:
                return

            current_time = self.clock.current_time()
            equity_curves: deque = self.statistics_manager.get_equity_curves(exchange)
            if len(equity_curves) != 0:
                if len(equity_curves) == 1:
                    equity_curves.appendleft(
                        (
                            current_time - timedelta(minutes=2),
                            self.broker_manager.get_broker(exchange).get_portfolio_total_equity(),
                        )
                    )
                most_recent_time = equity_curves[-1][0]
                if most_recent_time >= current_time:
                    return

            equity_curves.append(
                (
                    current_time,
                    self.broker_manager.get_broker(exchange).get_portfolio_total_equity(),
                )
            )

    def update_equity_dfs(self):
        """
        It takes the equity curves from the statistics manager and puts them into a dataframe
        """
        exchanges = {asset.exchange for asset in self.assets}
        self.equity_dfs = {}
        LOG.info("exchanges = %s", exchanges)
        for exchange in exchanges:
            equity_df = pd.DataFrame(
                list(self.statistics_manager.get_equity_curves(exchange)),
                columns=["Timestamp", "Equity"],
            ).set_index("Timestamp")
            self.statistics_manager.set_equity_dfs(equity_df, exchange)
            self.equity_dfs[exchange] = equity_df

    def update_positions(self):
        """
        It updates the positions of the assets in the portfolio
        """
        exchanges = {asset.exchange for asset in self.assets}
        for exchange in exchanges:
            if self.statistics_manager.get_positions(exchange) is None:
                self.statistics_manager.set_positions([], exchange)

            if not self.clock.updated:
                return

            current_time = self.clock.current_time()
            if len(self.statistics_manager.get_positions(exchange)) != 0:
                most_recent_time = self.statistics_manager.get_positions(exchange)[-1][
                    0
                ]
                if most_recent_time >= current_time:
                    return

            portfolio_dict = self.broker_manager.get_broker(exchange).get_portfolio_as_dict()

            positions = []
            new_pos = False
            for asset in self.assets:
                for direction in Direction:
                    if asset.key() not in portfolio_dict:
                        positions.append(
                            (
                                asset.exchange,
                                asset.symbol,
                                direction.name,
                                0,
                                0,
                            )
                        )
                        continue
                    market_value = (
                        portfolio_dict[asset.key()][direction]["market_value"]
                        if direction in portfolio_dict[asset.key()]
                        else 0
                    )
                    size = (
                        portfolio_dict[asset.key()][direction]["size"]
                        if direction in portfolio_dict[asset.key()]
                        else 0
                    )
                    if size != 0:
                        new_pos = True
                        positions.append(
                            (
                                asset.exchange,
                                asset.symbol,
                                direction.name,
                                market_value,
                                size,
                            )
                        )

            if not new_pos:
                continue

            cash = self.broker_manager.get_broker(exchange).get_portfolio_cash_balance()
            for position in positions:
                self.statistics_manager.get_positions(exchange).append(
                    (
                        current_time,
                        *position,
                        cash,
                    )
                )

    def update_positions_dfs(self):
        """
        > It takes the positions from the statistics manager and creates a dataframe for each exchange
        """
        exchanges = {asset.exchange for asset in self.assets}
        self.positions_dfs = {}
        for exchange in exchanges:
            positions_df = pd.DataFrame(
                self.statistics_manager.get_positions(exchange),
                columns=[
                    "Timestamp",
                    "Exchange",
                    "Symbol",
                    "Direction",
                    "Market value",
                    "Size",
                    "Cash",
                ],
            ).set_index("Timestamp")
            self.statistics_manager.set_positions_dfs(positions_df, exchange)
            self.positions_dfs[exchange] = positions_df

    def update_transactions(self):
        """
        > If the clock has been updated, and the most recent transaction time is less than the current time, then update the
        transactions
        """
        exchanges = {asset.exchange for asset in self.assets}
        for exchange in exchanges:
            if not self.clock.updated:
                return

            current_time = self.clock.current_time()
            if (
                self.statistics_manager.get_transactions(exchange) is not None
                and len(self.statistics_manager.get_transactions(exchange)) != 0
            ):
                most_recent_time = self.statistics_manager.get_transactions(exchange)[
                    -1
                ][0]
                if most_recent_time >= current_time:
                    return

            portfolio = self.broker_manager.get_broker(exchange).portfolio
            self.statistics_manager.set_transactions(
                [
                    (
                        transaction.timestamp,
                        transaction.size
                        if transaction.action == Action.BUY
                        else -transaction.size,
                        transaction.price,
                        transaction.asset.key(),
                    )
                    for transaction in portfolio.transactions
                ],
                exchange,
            )

    def update_transactions_dfs(self):
        """
        It takes the transactions from the statistics manager and puts them into a dataframe
        """
        exchanges = {asset.exchange for asset in self.assets}
        self.transactions_dfs = {}
        for exchange in exchanges:
            if self.statistics_manager.get_transactions(exchange) is not None:
                transactions_df = pd.DataFrame(
                    self.statistics_manager.get_transactions(exchange),
                    columns=["Timestamp", "amount", "price", "symbol"],
                ).set_index("Timestamp")
                self.statistics_manager.set_transactions_dfs(transactions_df, exchange)
            else:
                transactions_df = pd.DataFrame(
                    columns=["Timestamp", "amount", "price", "symbol"]
                ).set_index("Timestamp")
                self.statistics_manager.set_transactions_dfs(
                    transactions_df,
                    exchange,
                )
            self.transactions_dfs[exchange] = transactions_df

    def run_strategies(self):
        """
        It runs the strategy, then logs the cash balance, portfolio, and total equity for each exchange
        """
        for strategy in self.strategy_instances:
            self.run_strategy(strategy)
            exchanges = {asset.exchange for asset in self.context.candles}
            for exchange in exchanges:
                LOG.info(
                    "%s: Strategy %s results: cash = %s, portfolio = %s, total_equity = %s",
                    exchange,
                    strategy,
                    self.broker_manager.get_broker(exchange).get_portfolio_cash_balance(),
                    self.context.broker_manager.get_broker(exchange).get_portfolio_as_dict(),
                    self.context.broker_manager.get_broker(exchange).get_portfolio_total_equity(),
                )

    def handle_asset_delayed_events(self, candle: Candle):
        """
        If there are any delayed events for the asset in the current candle, decrement the delay counter for each delayed
        event and if the delay counter reaches zero, add the event to the events queue.

        :param candle: The candle that was just received
        :type candle: Candle
        """
        asset_delayed_events = []
        if candle.asset in self.asset_delayed_events:
            for event in self.asset_delayed_events[candle.asset]:
                event.bars_delay -= 1
                if event.bars_delay == 0:
                    self.events.append(event)
                else:
                    asset_delayed_events.append(event)
            self.asset_delayed_events[candle.asset] = asset_delayed_events

    def handle_delayed_events(self):
        """
        It takes all the events that are delayed, and if their delay is up, it adds them to the list of events to be
        processed
        """
        delayed_events = []
        for event in self.delayed_events:
            event.bars_delay -= 1
            if event.bars_delay == 0:
                self.events.append(event)
            else:
                delayed_events.append(event)
        self.delayed_events = delayed_events

    def add_signal(self, signal: SignalBase):
        if isinstance(signal, Signal):
            self.signals[signal.asset][signal.time_unit].append(signal)
        elif isinstance(signal, MultipleSignal):
            for signal_base in signal.signals:
                self.add_signal(signal_base)

    def __init__(
        self,
        events: deque,
        assets: dict[Asset, timedelta | list[timedelta]],
        feed: Feed,
        order_manager: OrderManager,
        strategies_parameters: dict[type, dict[str, Any]] = None,
        indicator_mode: IndicatorMode = IndicatorMode.BATCH,
        mode: EventLoopMode = EventLoopMode.BATCH,
        close_at_end_of_day=True,
        close_at_end_of_data=True,
        broker_isolation=BrokerIsolation.EXCHANGE,
        statistics_class: type = None,
        real_time_plotting=False,
    ):
        self.events: deque = events
        self.asset_delayed_events = {}
        self.delayed_events = []
        self.assets = normalize_assets(assets)
        self.feed = feed
        self.indicators = ReactiveIndicators(mode=indicator_mode, memoize=True)
        self.data = CandleData(candles=feed.candles, indicators=self.indicators)
        self.order_manager = order_manager
        self.broker_manager = self.order_manager.broker_manager
        self.clock = self.order_manager.clock
        self.strategies_parameters = strategies_parameters if strategies_parameters is not None else {}
        self.strategy_instances: list[StrategyBase] = []
        self.context = Context(
            assets=self.assets,
            order_manager=self.order_manager,
            broker_manager=self.broker_manager,
            events=self.events,
        )
        self.indicator_mode = indicator_mode
        self.mode = mode
        self._init_strategy_instances()
        self.seen_candles = {}
        self._init_seen_candles()
        self.close_at_end_of_day = close_at_end_of_day
        self.close_at_end_of_data = close_at_end_of_data
        self.last_update = None
        self.signals_and_orders_last_update = None
        self.current_timestamp = MAX_TIMESTAMP
        self.broker_isolation = broker_isolation
        self.statistics_manager = StatisticsManager(isolation=self.broker_isolation)
        self.statistics_class = statistics_class
        self.statistics_df = None
        self.signals = {
            asset: {time_unit: [] for time_unit in self.assets[asset]}
            for asset in self.assets
        }
        self.signals_df = {asset: {} for asset in self.assets}
        self.orders = self.order_manager.orders
        self.orders_df = None
        self.real_time_plotting = real_time_plotting
        self.figs = None
        self.trading_event_trace_index = {}
        self.indicator_trace_index = {}

    def loop(self):
        """
        The function loops through the events queue and processes each event
        """
        data_to_process = True
        while data_to_process:
            if self.mode == EventLoopMode.LIVE:
                # tasks to be done regularly
                for exchange in self.broker_manager.brokers:
                    self.broker_manager.get_broker(exchange).synchronize()
                now = self.clock.current_time()
                if (
                    self.signals_and_orders_last_update is None
                    or now - self.signals_and_orders_last_update
                    >= timedelta(seconds=10)
                ):
                    self.order_manager.process_pending_signals()
                    for exchange in self.broker_manager.brokers:
                        self.broker_manager.get_broker(exchange).execute_open_orders()
                    self.signals_and_orders_last_update = self.clock.current_time()

                now = self.clock.current_time()
                if self.last_update is not None and now - self.last_update < timedelta(
                    minutes=1
                ):
                    continue

                self.last_update = self.clock.current_time()

            self.feed.update_latest_data()

            # Handle the events
            while True:
                if len(self.events) == 0:
                    break
                event = self.events.popleft()
                if event is None:
                    continue

                if event.bars_delay > 0:
                    if isinstance(event, AssetSpecificEvent):
                        if event.asset not in self.asset_delayed_events:
                            self.asset_delayed_events[event.asset] = [event]
                        else:
                            self.asset_delayed_events[event.asset].append(event)
                    elif isinstance(event, DataEvent) or isinstance(
                        event, PendingSignalEvent
                    ):
                        self.delayed_events.append(event)
                    continue

                match event.event_type:
                    case EventType.MARKET_DATA:
                        candles_dict = event.candles
                        eod_assets = {}
                        for asset in candles_dict:
                            for time_unit in candles_dict[asset]:
                                candles = candles_dict[asset][time_unit]
                                for candle in candles:
                                    timestamp_str = str(candle.timestamp)
                                    if (
                                        timestamp_str
                                        in self.seen_candles[candle.asset][candle.time_unit]
                                    ):
                                        continue
                                    self.seen_candles[candle.asset][candle.time_unit].add(
                                        timestamp_str
                                    )
                                    self.context.add_candle(candle)
                                    self.handle_asset_delayed_events(candle)
                                    self.handle_delayed_events()
                                    if self.close_at_end_of_day and self.clock.end_of_day():
                                        if asset not in eod_assets:
                                            eod_assets[asset] = []
                                        eod_assets[asset].append(candle.time_unit)
                        candles_are_processed = False
                        while not candles_are_processed:
                            self.context.update()
                            last_candles = self.context.get_last_candles()
                            if len(last_candles) == 0:
                                candles_are_processed = True
                                break
                            self.update_equity_curves()
                            self.update_positions()
                            self.clock.update(
                                self.context.current_timestamp + timedelta(minutes=1)
                            )
                            for candle in last_candles:
                                LOG.info("Process new candle: %s", candle.to_json())
                                self.data(candle.asset, candle.time_unit).push(candle)
                                if (
                                    self.mode == EventLoopMode.LIVE
                                    and self.real_time_plotting
                                ):
                                    self.real_time_plot(candle.asset, candle.time_unit)
                                self.broker_manager.get_broker(candle.asset.exchange).update_price(candle)
                            self.run_strategies()
                            for candle in last_candles:
                                self.broker_manager.get_broker(candle.asset.exchange).execute_open_orders()
                        if len(eod_assets) != 0:
                            bars_delay = 0
                            if self.mode != EventLoopMode.LIVE:
                                bars_delay = 1
                            timestamp = min([self.clock.current_time() for _ in eod_assets])
                            self.events.append(
                                MarketEodDataEvent(
                                    assets=eod_assets,
                                    timestamp=timestamp,
                                    bars_delay=bars_delay,
                                )
                            )
                    case EventType.OPEN_ORDERS:
                        for exchange in self.broker_manager.brokers:
                            self.broker_manager.get_broker(exchange).execute_open_orders()
                    case EventType.PENDING_SIGNAL:
                        self.order_manager.process_pending_signals()
                    case EventType.MARKET_EOD_DATA:
                        assets = event.assets
                        for asset in assets:
                            self.broker_manager.get_broker(asset.exchange).close_all_open_positions(asset)
                    case EventType.SIGNAL:
                        signals = event.signals
                        for signal in signals:
                            self.add_signal(signal)
                            self.order_manager.check_signal(signal)
                    case EventType.MARKET_DATA_END:
                        if self.close_at_end_of_data:
                            assets = event.assets
                            for asset in assets:
                                self.broker_manager.get_broker(asset.exchange).close_all_open_positions(asset=asset, end_of_day=False)
                        self.update_equity_curves()
                        self.update_positions()
                        self.update_equity_dfs()
                        self.update_positions_dfs()
                        self.update_transactions()
                        self.update_transactions_dfs()
                        self.update_signals_df()
                        self.order_manager.update_orders_df()
                        self.orders_df = self.order_manager.orders_df

                        exchanges = [asset.exchange for asset in self.assets]
                        if self.statistics_class is not None:
                            for exchange in exchanges:
                                if self.statistics_manager.get_equity_dfs(exchange).empty:
                                    self.statistics_df = pd.DataFrame()
                                    continue
                                self.statistics_df = self.statistics_class(
                                    equity=self.statistics_manager.get_equity_dfs(exchange),
                                    positions=self.statistics_manager.get_positions_dfs(
                                        exchange
                                    ),
                                    transactions=self.statistics_manager.get_transactions_dfs(
                                        exchange
                                    ),
                                ).get_tearsheet()
                        data_to_process = False
                        break

    def plot_indicators_instances_graph(self):
        self.indicators.plot_instances_graph()

    def update_signals_df(self):
        for asset in self.assets:
            for time_unit in self.assets[asset]:
                self.signals_df[asset][time_unit] = pd.DataFrame(
                    [
                        [
                            signal.generation_time,
                            signal.asset.exchange,
                            signal.asset.symbol,
                            str(signal.time_unit),
                            signal.action.name,
                            signal.direction.name,
                            str(signal.time_in_force),
                            signal.root_candle_timestamp,
                        ]
                        for signal in self.signals[asset][time_unit]
                    ],
                    columns=[
                        "Generation time",
                        "Exchange",
                        "Symbol",
                        "Time unit",
                        "Action",
                        "Direction",
                        "Time in force",
                        "Root candle timestamp",
                    ],
                ).set_index("Generation time")

    @staticmethod
    def get_volumes_data(
        candle_dataframe: CandleDataFrame,
    ) -> list[Tuple]:
        green_candle_dataframe = candle_dataframe[
            candle_dataframe["open"] < candle_dataframe["close"]
        ]
        red_candle_dataframe = candle_dataframe[
            candle_dataframe["open"] > candle_dataframe["close"]
        ]
        grey_candle_dataframe = candle_dataframe[
            candle_dataframe["open"] == candle_dataframe["close"]
        ]

        volumes_data = [
            ("buying volume", green_candle_dataframe, "green"),
            ("selling volume", red_candle_dataframe, "red"),
            ("balanced volume", grey_candle_dataframe, "grey"),
        ]
        return volumes_data

    def compute_trading_events_data(self, candle_dataframe: CandleDataFrame) -> Tuple:
        last_timestamp = candle_dataframe.iloc[-1].name

        # signals
        if (
            self.signals is not None
            and candle_dataframe.asset in self.signals
            and candle_dataframe.time_unit in self.signals[candle_dataframe.asset]
        ):
            buy_signals: list[Signal] = [
                signal
                for signal in self.signals[candle_dataframe.asset][
                    candle_dataframe.time_unit
                ]
                if signal.is_entry_signal and signal.generation_time <= last_timestamp
            ]
            buy_signals_timestamps = [signal.generation_time for signal in buy_signals]
            buy_signal_candle_dataframe = candle_dataframe.loc[buy_signals_timestamps]
            buy_signal_candle_dataframe = 0.9985 * buy_signal_candle_dataframe[["low"]]
            sell_signals: list[Signal] = [
                signal
                for signal in self.signals[candle_dataframe.asset][
                    candle_dataframe.time_unit
                ]
                if signal.is_exit_signal and signal.generation_time <= last_timestamp
            ]
            sell_signals_timestamps = [
                signal.generation_time for signal in sell_signals
            ]
            sell_signal_candle_dataframe = candle_dataframe.loc[sell_signals_timestamps]
            sell_signal_candle_dataframe = (
                1.0015 * sell_signal_candle_dataframe[["high"]]
            )
        else:
            buy_signals = sell_signals = []
            buy_signal_candle_dataframe = (
                sell_signal_candle_dataframe
            ) = CandleDataFrame(
                asset=candle_dataframe.asset, time_unit=candle_dataframe.time_unit
            )

        # orders
        if (
            self.orders is not None
            and candle_dataframe.asset in self.orders
            and candle_dataframe.time_unit in self.orders[candle_dataframe.asset]
        ):
            buy_orders: list[Order] = [
                order
                for order in self.orders[candle_dataframe.asset][
                    candle_dataframe.time_unit
                ]
                if order.is_entry_order and order.generation_time <= last_timestamp
            ]
            buy_orders_timestamps = [order.generation_time for order in buy_orders]
            buy_order_candle_dataframe = candle_dataframe.loc[buy_orders_timestamps]
            buy_order_candle_dataframe = 0.9975 * buy_order_candle_dataframe[["low"]]
            sell_orders: list[Order] = [
                order
                for order in self.orders[candle_dataframe.asset][
                    candle_dataframe.time_unit
                ]
                if order.is_exit_order and order.generation_time <= last_timestamp
            ]
            sell_orders_timestamps = [order.generation_time for order in sell_orders]
            sell_order_candle_dataframe = candle_dataframe.loc[sell_orders_timestamps]
            sell_order_candle_dataframe = 1.0025 * sell_order_candle_dataframe[["high"]]
        else:
            buy_orders = sell_orders = []
            buy_order_candle_dataframe = sell_order_candle_dataframe = CandleDataFrame(
                asset=candle_dataframe.asset, time_unit=candle_dataframe.time_unit
            )

        # transactions
        portfolio = self.broker_manager.get_broker(candle_dataframe.asset.exchange).portfolio
        transactions: list[Transaction] = portfolio.transactions
        buy_transactions = [
            transaction
            for transaction in transactions
            if transaction.is_entry_transaction
            and transaction.timestamp <= last_timestamp
        ]
        buy_transactions_timestamps = [
            transaction.timestamp for transaction in buy_transactions
        ]
        buy_transaction_candle_dataframe = candle_dataframe.loc[
            buy_transactions_timestamps
        ]
        buy_transaction_candle_dataframe = (
            1.002 * buy_transaction_candle_dataframe[["low"]]
        )
        sell_transactions = [
            transaction
            for transaction in transactions
            if transaction.is_exit_transaction
            and transaction.timestamp <= last_timestamp
        ]
        sell_transactions_timestamps = [
            transaction.timestamp for transaction in sell_transactions
        ]
        sell_transaction_candle_dataframe = candle_dataframe.loc[
            sell_transactions_timestamps
        ]
        sell_transaction_candle_dataframe = (
            0.998 * sell_transaction_candle_dataframe[["high"]]
        )

        return (
            buy_signal_candle_dataframe,
            buy_signals,
            sell_signal_candle_dataframe,
            sell_signals,
            buy_order_candle_dataframe,
            buy_orders,
            sell_order_candle_dataframe,
            sell_orders,
            buy_transaction_candle_dataframe,
            buy_transactions,
            sell_transaction_candle_dataframe,
            sell_transactions,
        )

    def get_trading_events_data(self, candle_dataframe: CandleDataFrame) -> list[Tuple]:
        (
            buy_signal_candle_dataframe,
            buy_signals,
            sell_signal_candle_dataframe,
            sell_signals,
            buy_order_candle_dataframe,
            buy_orders,
            sell_order_candle_dataframe,
            sell_orders,
            buy_transaction_candle_dataframe,
            buy_transactions,
            sell_transaction_candle_dataframe,
            sell_transactions,
        ) = self.compute_trading_events_data(candle_dataframe)

        def signal_text(signal: Signal):
            return (
                f"signal<br>"
                f"root candle timestamp: {signal.root_candle_timestamp}<br>"
                f"strategy: {signal.strategy}<br>"
                f"time in force: {signal.time_in_force}"
            )

        def order_text(order: Order):
            return (
                f"order<br>"
                f"size: {order.size}<br>"
                f"type: {order.order_type.name}<br>"
                f"condition: {order.condition.name}<br>"
                f"time in force: {order.time_in_force}<br>"
                f"status: {order.status.name}<br>"
                f"order_id: {order.order_id}<br>"
                f"signal id: {order.signal_id}<br>"
            )

        def transaction_text(transaction: Transaction):
            return (
                f"transaction<br>"
                f"size: {transaction.size}<br>"
                f"price: {transaction.price}<br>"
                f"commission: {transaction.commission}<br>"
                f"order_id: {transaction.order_id}<br>"
                f"transaction_id: {transaction.transaction_id}<br>"
            )

        trading_events_data = [
            (
                "buy signal",
                buy_signal_candle_dataframe,
                "triangle-up-dot",
                "green",
                signal_text,
                buy_signals,
                "low",
            ),
            (
                "sell signal",
                sell_signal_candle_dataframe,
                "triangle-down-dot",
                "red",
                signal_text,
                sell_signals,
                "high",
            ),
            (
                "buy order",
                buy_order_candle_dataframe,
                "circle-dot",
                "green",
                order_text,
                buy_orders,
                "low",
            ),
            (
                "sell order",
                sell_order_candle_dataframe,
                "circle-dot",
                "red",
                order_text,
                sell_orders,
                "high",
            ),
            (
                "buy transaction",
                buy_transaction_candle_dataframe,
                "hexagram-dot",
                "green",
                transaction_text,
                buy_transactions,
                "low",
            ),
            (
                "sell transaction",
                sell_transaction_candle_dataframe,
                "hexagram-dot",
                "red",
                transaction_text,
                sell_transactions,
                "high",
            ),
        ]
        return trading_events_data

    def plot_base_chart(
        self, candle_dataframe: CandleDataFrame, volumes_data, trading_events_data
    ):
        fig: go.Figure = self.figs[candle_dataframe.asset][candle_dataframe.time_unit]
        fig.add_trace(
            go.Candlestick(
                name="candles",
                x=candle_dataframe.index,
                open=candle_dataframe["open"],
                high=candle_dataframe["high"],
                low=candle_dataframe["low"],
                close=candle_dataframe["close"],
            ),
            row=1,
            col=1,
        )

        # volumes
        for name, volume_dataframe, color in volumes_data:
            fig.add_trace(
                go.Bar(
                    name=name,
                    x=volume_dataframe.index,
                    y=volume_dataframe["volume"],
                    marker={
                        "color": color,
                    },
                ),
                row=2,
                col=1,
            )

        for (
            name,
            trading_event_dataframe,
            symbol,
            color,
            text_function,
            values,
            column,
        ) in trading_events_data:
            if not trading_event_dataframe.empty:
                fig.add_trace(
                    go.Scatter(
                        name=name,
                        x=trading_event_dataframe.index,
                        y=trading_event_dataframe[column],
                        mode="markers",
                        marker=dict(symbol=symbol, size=12, color=color),
                        hovertemplate="%{text}",
                        text=[text_function(value) for value in values],
                    ),
                    row=1,
                    col=1,
                )

        # # set title
        fig.update_layout(
            hoverlabel_align="right",
            title=f"{candle_dataframe.asset.key()} chart and signals",
            yaxis1_title="OHLC",
            yaxis2_title="Volume",
            xaxis2_title="Time",
            xaxis1_rangeslider_visible=False,
            xaxis2_rangeslider_visible=False,
        )

    def init_figs(self):
        if self.figs is None:
            self.figs = {
                asset: {
                    time_unit: go.FigureWidget(
                        make_subplots(
                            rows=2,
                            cols=1,
                            shared_xaxes=True,
                            vertical_spacing=0.02,
                            row_width=[0.2, 0.7],
                        )
                    )
                    for time_unit in self.assets[asset]
                }
                for asset in self.assets
            }

    def real_time_plot(self, asset: Asset, time_unit: timedelta):
        self.init_figs()
        if asset not in self.figs or time_unit not in self.figs[asset]:
            return
        candle_indicator = self.data(asset, time_unit)
        candle_dataframe: CandleDataFrame = CandleDataFrame.from_candle_list(
            asset, candle_indicator.get_ordered_window()
        )
        if candle_dataframe.empty:
            return
        candle_dataframe["open"] = pd.to_numeric(candle_dataframe["open"])
        candle_dataframe["high"] = pd.to_numeric(candle_dataframe["high"])
        candle_dataframe["low"] = pd.to_numeric(candle_dataframe["low"])
        candle_dataframe["close"] = pd.to_numeric(candle_dataframe["close"])

        volumes_data = self.get_volumes_data(candle_dataframe)
        trading_events_data = self.get_trading_events_data(candle_dataframe)

        fig = self.figs[asset][time_unit]

        if len(fig.data) == 0:
            # chart
            self.plot_base_chart(candle_dataframe, volumes_data, trading_events_data)
            # add indicators
            for instance in self.indicators.instances:
                instance_trace = instance.get_trace(candle_dataframe.index)
                if instance_trace is not None:
                    fig.add_trace(instance_trace)
        else:
            fig.data[0].x = candle_dataframe.index
            fig.data[0].open = candle_dataframe["open"]
            fig.data[0].high = candle_dataframe["high"]
            fig.data[0].low = candle_dataframe["low"]
            fig.data[0].close = candle_dataframe["close"]

            for index, volume_stats in enumerate(volumes_data):
                name, volume_dataframe, color = volume_stats
                fig.data[index + 1].x = volume_dataframe.index
                fig.data[index + 1].y = volume_dataframe["volume"]

            for index, trading_event_datum in enumerate(trading_events_data):
                (
                    name,
                    trading_event_dataframe,
                    symbol,
                    color,
                    text_function,
                    values,
                    column,
                ) = trading_event_datum
                if (
                    name not in self.trading_event_trace_index
                    and not trading_event_dataframe.empty
                ):
                    self.trading_event_trace_index[name] = len(fig.data)
                    fig.add_trace(
                        go.Scatter(
                            name=name,
                            x=trading_event_dataframe.index,
                            y=trading_event_dataframe[column],
                            mode="markers",
                            marker=dict(symbol=symbol, size=12, color=color),
                            hovertemplate="%{text}",
                            text=[text_function(value) for value in values],
                        ),
                        row=1,
                        col=1,
                    )
                elif name in self.trading_event_trace_index:
                    fig.data[
                        self.trading_event_trace_index[name]
                    ].x = trading_event_dataframe.index
                    fig.data[
                        self.trading_event_trace_index[name]
                    ].y = trading_event_dataframe[column]
                    fig.data[self.trading_event_trace_index[name]].text = [
                        text_function(value) for value in values
                    ]

                # indicators
                for instance in self.indicators.instances:
                    instance_trace = instance.get_trace(candle_dataframe.index)
                    if instance.id not in self.indicator_trace_index:
                        if instance_trace is not None:
                            self.indicator_trace_index[instance.id] = len(fig.data)
                            fig.add_trace(instance_trace)
                    else:
                        if instance_trace is not None:
                            fig_instance_trace = fig.data[
                                self.indicator_trace_index[instance.id]
                            ]
                            for attribute in instance.plotting_attributes():
                                setattr(
                                    fig_instance_trace,
                                    attribute,
                                    getattr(instance_trace, attribute),
                                )

    def plot(self, asset: Asset, time_unit: timedelta):
        self.init_figs()
        if asset not in self.figs or time_unit not in self.figs[asset]:
            return
        candle_dataframe: CandleDataFrame = self.feed.candle_dataframes[asset][
            time_unit
        ]
        if candle_dataframe.empty:
            return
        candle_dataframe["open"] = pd.to_numeric(candle_dataframe["open"])
        candle_dataframe["high"] = pd.to_numeric(candle_dataframe["high"])
        candle_dataframe["low"] = pd.to_numeric(candle_dataframe["low"])
        candle_dataframe["close"] = pd.to_numeric(candle_dataframe["close"])
        volumes_data = self.get_volumes_data(candle_dataframe)
        trading_events_data = self.get_trading_events_data(candle_dataframe)

        self.plot_base_chart(candle_dataframe, volumes_data, trading_events_data)
        fig = self.figs[asset][time_unit]

        # add indicators
        for instance in self.indicators.instances:
            instance_trace = instance.get_trace(candle_dataframe.index)
            if instance_trace is not None:
                fig.add_trace(instance_trace)

        fig.show()
