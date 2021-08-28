import os
from collections import deque
from datetime import timedelta
from threading import Thread
from typing import Any, Dict, List

import pandas as pd

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.common.constants import MAX_TIMESTAMP
from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.feed.feed import Feed
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction, EventType
from trazy_analysis.models.event import (
    AssetSpecificEvent,
    DataEvent,
    MarketEodDataEvent,
    PendingSignalEvent,
)
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.strategy.context import Context
from trazy_analysis.strategy.strategy import Strategy

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


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


class EventLoop:
    def _init_strategy_instance(
        self, strategy_class: type, parameters_list: List[Dict[str, float]]
    ):
        if issubclass(strategy_class, Strategy):
            for parameters in parameters_list:
                self.strategy_instances.append(
                    strategy_class(
                        self.context,
                        self.order_manager,
                        self.events,
                        parameters,
                        self.indicators_manager,
                    )
                )

    def _init_strategy_instances(self):
        for strategy_class in self.strategies_parameters:
            parameters_list = self.strategies_parameters[strategy_class]
            self._init_strategy_instance(strategy_class, parameters_list)

    def _init_seen_candles(self):
        for asset in self.assets:
            self.seen_candles[asset] = ExpiringSet()

    def run_strategy(self, strategy: Strategy):
        strategy.process_context(self.context, self.clock)

    def update_equity_curves(self):
        exchanges = {asset.exchange for asset in self.assets}
        for strategy in self.strategy_instances:
            for exchange in exchanges:
                get_or_create_nested_dict(self.equity_curves, strategy.name)
                if exchange not in self.equity_curves[strategy.name]:
                    self.equity_curves[strategy.name][exchange] = []

                if not self.clock.updated:
                    return

                current_time = self.clock.current_time()
                if len(self.equity_curves[strategy.name][exchange]) != 0:
                    most_recent_time = self.equity_curves[strategy.name][exchange][-1][
                        0
                    ]
                    if most_recent_time >= current_time:
                        return

                self.equity_curves[strategy.name][exchange].append(
                    (
                        current_time,
                        self.broker_manager.get_broker(
                            exchange
                        ).get_portfolio_total_equity(),
                    )
                )

    def update_equity_dfs(self):
        exchanges = {asset.exchange for asset in self.assets}
        for strategy in self.strategy_instances:
            for exchange in exchanges:
                get_or_create_nested_dict(self.equity_dfs, strategy.name)
                equity_df = pd.DataFrame(
                    self.equity_curves[strategy.name][exchange],
                    columns=["Date", "Equity"],
                ).set_index("Date")
                self.equity_dfs[strategy.name][exchange] = equity_df

    def update_positions(self):
        exchanges = {asset.exchange for asset in self.assets}
        for exchange in exchanges:
            if exchange not in self.positions:
                self.positions[exchange] = []

            if not self.clock.updated:
                return

            current_time = self.clock.current_time()
            if len(self.positions[exchange]) != 0:
                most_recent_time = self.positions[exchange][-1][0]
                if most_recent_time >= current_time:
                    return

            portfolio_dict = self.broker_manager.get_broker(
                exchange
            ).get_portfolio_as_dict()

            positions = []
            new_pos = False
            for asset in self.assets:
                if asset.key() not in portfolio_dict:
                    positions.append(0)
                    continue
                long_pos = (
                    portfolio_dict[asset.key()][Direction.LONG]["market_value"]
                    if Direction.LONG in portfolio_dict[asset.key()]
                    else 0
                )
                short_pos = (
                    portfolio_dict[asset.key()][Direction.SHORT]["market_value"]
                    if Direction.SHORT in portfolio_dict[asset.key()]
                    else 0
                )
                net_pos = long_pos + short_pos
                if net_pos != 0:
                    new_pos = True
                positions.append(long_pos + short_pos)

            if not new_pos:
                continue

            cash = self.broker_manager.get_broker(exchange).get_portfolio_cash_balance()
            self.positions[exchange].append(
                (
                    current_time,
                    *positions,
                    cash,
                )
            )

    def update_positions_dfs(self):
        exchanges = {asset.exchange for asset in self.assets}
        for exchange in exchanges:
            positions_df = pd.DataFrame(
                self.positions[exchange],
                columns=["Date", *[asset.key() for asset in self.assets], "cash"],
            ).set_index("Date")
            self.positions_dfs[exchange] = positions_df

    def update_transactions(self):
        exchanges = {asset.exchange for asset in self.assets}
        for exchange in exchanges:
            if not self.clock.updated:
                return

            current_time = self.clock.current_time()
            if exchange in self.transactions and len(self.transactions[exchange]) != 0:
                most_recent_time = self.transactions[exchange][-1][0]
                if most_recent_time >= current_time:
                    return

            portfolio = self.broker_manager.get_broker(exchange).portfolio
            self.transactions[exchange] = [
                (
                    current_time,
                    transaction.size
                    if transaction.action == Action.BUY
                    else -transaction.size,
                    transaction.price,
                    transaction.asset.key(),
                )
                for transaction in portfolio.transactions
            ]

    def update_transactions_dfs(self):
        exchanges = {asset.exchange for asset in self.assets}
        for exchange in exchanges:
            transactions_df = pd.DataFrame(
                self.transactions[exchange],
                columns=["Date", "amount", "price", "symbol"],
            ).set_index("Date")
            self.transactions_dfs[exchange] = transactions_df

    def run_strategies(self):
        for strategy in self.strategy_instances:
            self.run_strategy(strategy)
            exchanges = {asset.exchange for asset in self.context.candles}
            for exchange in exchanges:
                LOG.info(
                    "%s: Strategy %s results: cash = %s, portfolio = %s, total_equity = %s",
                    exchange,
                    strategy,
                    self.broker_manager.get_broker(
                        exchange
                    ).get_portfolio_cash_balance(),
                    strategy.order_manager.broker_manager.get_broker(
                        exchange
                    ).get_portfolio_as_dict(),
                    strategy.order_manager.broker_manager.get_broker(
                        exchange
                    ).get_portfolio_total_equity(),
                )

    def handle_asset_delayed_events(self, candle: Candle):
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
        delayed_events = []
        for event in self.delayed_events:
            event.bars_delay -= 1
            if event.bars_delay == 0:
                self.events.append(event)
            else:
                delayed_events.append(event)
        self.delayed_events = delayed_events

    def __init__(
        self,
        events: deque,
        assets: List[Asset],
        feed: Feed,
        order_manager: OrderManager,
        indicators_manager: IndicatorsManager,
        strategies_parameters: Dict[type, List[Dict[str, float]]] = {},
        live=False,
        close_at_end_of_day=True,
        close_at_end_of_data=True,
    ):
        self.events: deque = events
        self.asset_delayed_events = {}
        self.delayed_events = []
        self.assets = sorted(set(assets))
        self.feed = feed
        self.order_manager = order_manager
        self.broker_manager = self.order_manager.broker_manager
        self.clock = self.order_manager.clock
        self.indicators_manager = indicators_manager
        self.strategies_parameters = strategies_parameters
        self.strategy_instances: List[Strategy] = []
        self.context = Context(assets=self.assets)
        self._init_strategy_instances()
        self.indicators_manager.warmup()
        self.seen_candles = {}
        self._init_seen_candles()
        self.live = live
        self.close_at_end_of_day = close_at_end_of_day
        self.close_at_end_of_data = close_at_end_of_data
        self.last_update = None
        self.signals_and_orders_last_update = None
        self.current_timestamp = MAX_TIMESTAMP
        self.equity_curves = {}
        self.equity_dfs = {}
        self.positions = {}
        self.positions_dfs = {}
        self.transactions = {}
        self.transactions_dfs = {}

    def loop(self):
        data_to_process = True
        while data_to_process:
            if self.live:
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

                if event.type == EventType.MARKET_DATA:
                    candles_dict = event.candles
                    eod_assets = []
                    for asset in candles_dict:
                        candles = candles_dict[asset]
                        for candle in candles:
                            timestamp_str = str(candle.timestamp)
                            if timestamp_str in self.seen_candles[candle.asset]:
                                LOG.info(
                                    "Candle with asset %s and timestamp %s has already been processed",
                                    candle.asset,
                                    timestamp_str,
                                )
                                continue
                            self.seen_candles[candle.asset].add(timestamp_str)
                            self.context.add_candle(candle)
                            self.handle_asset_delayed_events(candle)
                            self.handle_delayed_events()
                            if self.close_at_end_of_day and self.clock.end_of_day():
                                eod_assets.append(candle.asset)
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
                            self.indicators_manager.RollingWindow(candle.asset).push(
                                candle
                            )
                            self.broker_manager.get_broker(
                                candle.asset.exchange
                            ).update_price(candle)
                        self.run_strategies()
                        for candle in last_candles:
                            self.broker_manager.get_broker(
                                candle.asset.exchange
                            ).execute_open_orders()
                    if len(eod_assets) != 0:
                        bars_delay = 0
                        if not self.live:
                            bars_delay = 1
                        timestamp = min(
                            [self.clock.current_time() for asset in eod_assets]
                        )
                        self.events.append(
                            MarketEodDataEvent(
                                assets=eod_assets,
                                timestamp=timestamp,
                                bars_delay=bars_delay,
                            )
                        )
                elif event.type == EventType.OPEN_ORDERS:
                    for exchange in self.broker_manager.brokers:
                        self.broker_manager.get_broker(exchange).execute_open_orders()
                elif event.type == EventType.PENDING_SIGNAL:
                    self.order_manager.process_pending_signals()
                elif event.type == EventType.MARKET_EOD_DATA:
                    assets = event.assets
                    for asset in assets:
                        self.broker_manager.get_broker(
                            asset.exchange
                        ).close_all_open_positions(asset)
                elif event.type == EventType.SIGNAL:
                    signals = event.signals
                    for signal in signals:
                        self.order_manager.check_signal(signal)
                elif event.type == EventType.MARKET_DATA_END:
                    if self.close_at_end_of_data:
                        assets = event.assets
                        for asset in assets:
                            self.broker_manager.get_broker(
                                asset.exchange
                            ).close_all_open_positions(asset=asset, end_of_day=False)
                    self.update_equity_curves()
                    self.update_positions()
                    self.update_equity_dfs()
                    self.update_positions_dfs()
                    self.update_transactions()
                    self.update_transactions_dfs()
                    data_to_process = False
                    break
