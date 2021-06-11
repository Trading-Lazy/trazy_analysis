import os
from collections import deque
from datetime import timedelta
from threading import Thread
from typing import Any, List

import logger
import settings
from common.constants import MAX_TIMESTAMP
from feed.feed import Feed
from indicators.indicators_manager import IndicatorsManager
from models.asset import Asset
from models.candle import Candle
from models.enums import EventType
from models.event import (
    AssetSpecificEvent,
    DataEvent,
    MarketEodDataEvent,
    PendingSignalEvent,
)
from order_manager.order_manager import OrderManager
from strategy.context import Context
from strategy.strategy import Strategy

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
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
    def _init_strategy_instance(self, strategy_class: type):
        if issubclass(strategy_class, Strategy):
            self.strategy_instances.append(
                strategy_class(
                    self.context,
                    self.order_manager,
                    self.events,
                    self.indicators_manager,
                )
            )

    def _init_strategy_instances(self):
        for strategy_class in self.strategies_classes:
            self._init_strategy_instance(strategy_class)

    def _init_seen_candles(self):
        for asset in self.assets:
            self.seen_candles[asset] = ExpiringSet()

    def run_strategy(self, strategy: Strategy):
        strategy.process_context(self.context, self.clock)

    def run_strategies(self):
        for strategy in self.strategy_instances:
            self.run_strategy(strategy)
        exchanges = {asset.exchange for asset in self.context.candles}
        for exchange in exchanges:
            LOG.info(
                "%s: Strategy results: cash = %s, portfolio = %s, total_equity = %s",
                exchange,
                strategy.order_manager.broker_manager.get_broker(
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
        strategies_classes: List[type] = [],
        live=False,
        close_at_end_of_day=True,
        close_at_end_of_data=True
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
        self.strategies_classes = strategies_classes
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
                        for candle in last_candles:
                            LOG.info("Process new candle: %s", candle.to_json())
                            self.indicators_manager.RollingWindow(candle.asset).push(
                                candle
                            )
                            self.clock.update(candle.timestamp + timedelta(minutes=1))
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
                    data_to_process = False
                    break
