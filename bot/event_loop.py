import os
from collections import deque
from datetime import timedelta
from threading import Thread
from typing import Any, List

import logger
import settings
from feed.feed import Feed
from indicators.indicators_manager import IndicatorsManager
from models.candle import Candle
from models.enums import EventType
from models.event import MarketEodDataEvent, SymbolSpecificEvent
from order_manager.order_manager import OrderManager
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
        for symbol in self.symbols:
            if issubclass(strategy_class, Strategy):
                self.strategy_instances.append(
                    strategy_class(
                        symbol,
                        self.order_manager,
                        self.events,
                        self.indicators_manager,
                    )
                )

    def _init_strategy_instances(self):
        for strategy_class in self.strategies_classes:
            self._init_strategy_instance(strategy_class)

    def _init_seen_candles(self):
        for symbol in self.symbols:
            self.seen_candles[symbol] = ExpiringSet()

    def run_strategy(self, strategy: Strategy, candle: Candle):
        strategy.process_candle(candle, self.clock)

    def run_strategies(self, candle: Candle):
        for strategy in self.strategy_instances:
            self.run_strategy(strategy, candle)
            LOG.info(
                "Strategy results: %s",
                strategy.order_manager.broker.get_portfolio_cash_balance(),
            )

    def handle_delayed_events(self, candle: Candle):
        symbol_delayed_events = []
        if candle.symbol in self.delayed_events:
            for event in self.delayed_events[candle.symbol]:
                event.bars_delay -= 1
                if event.bars_delay == 0:
                    self.events.append(event)
                else:
                    symbol_delayed_events.append(event)
            self.delayed_events[candle.symbol] = symbol_delayed_events

    def __init__(
        self,
        events: deque,
        symbols: List[str],
        feed: Feed,
        order_manager: OrderManager,
        indicators_manager: IndicatorsManager,
        strategies_classes: List[type] = [],
        live=False,
    ):
        self.events: deque = events
        self.delayed_events = {}
        self.symbols = sorted(set(symbols))
        self.feed = feed
        self.order_manager = order_manager
        self.broker = self.order_manager.broker
        self.clock = self.order_manager.clock
        self.indicators_manager = indicators_manager
        self.strategies_classes = strategies_classes
        self.strategy_instances: List[Strategy] = []
        self._init_strategy_instances()
        self.indicators_manager.warmup()
        self.seen_candles = {}
        self._init_seen_candles()
        self.live = live
        self.last_update = None
        self.signals_and_orders_last_update = None

    def loop(self):
        data_to_process = True
        while data_to_process:
            if self.live:
                # tasks to be done regularly
                self.broker.synchronize()
                now = self.clock.current_time()
                if (
                    self.signals_and_orders_last_update is None
                    or now - self.signals_and_orders_last_update
                    >= timedelta(seconds=10)
                ):
                    self.order_manager.process_pending_signals()
                    self.broker.execute_open_orders()
                    self.signals_and_orders_last_update = self.clock.current_time()

                now = self.clock.current_time()
                if self.last_update is not None and now - self.last_update < timedelta(
                    minutes=1
                ):
                    print("Toto")
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

                if isinstance(event, SymbolSpecificEvent):
                    if event.bars_delay > 0:
                        if event.symbol not in self.delayed_events:
                            self.delayed_events[event.symbol] = [event]
                        else:
                            self.delayed_events[event.symbol].append(event)
                        continue

                if event.type == EventType.MARKET_DATA:
                    candle = event.candle
                    timestamp_str = str(candle.timestamp)
                    if timestamp_str in self.seen_candles[candle.symbol]:
                        LOG.info(
                            "Candle with symbol %s and timestamp %s has already been processed",
                            candle.symbol,
                            timestamp_str,
                        )
                        continue
                    self.seen_candles[candle.symbol].add(timestamp_str)
                    LOG.info("Dequeue new candle: %s", candle.to_json())
                    self.indicators_manager.RollingWindow(candle.symbol).push(candle)
                    self.clock.update(
                        candle.symbol, candle.timestamp + timedelta(minutes=1)
                    )
                    self.broker.update_price(candle)
                    self.run_strategies(candle)
                    self.broker.execute_open_orders()
                    self.handle_delayed_events(candle)
                    if self.clock.end_of_day(candle.symbol):
                        bars_delay = 0
                        if not self.live:
                            bars_delay = 1
                        self.events.append(
                            MarketEodDataEvent(candle.symbol, bars_delay)
                        )
                elif event.type == EventType.OPEN_ORDERS:
                    self.broker.execute_open_orders()
                elif event.type == EventType.PENDING_SIGNAL:
                    self.order_manager.process_pending_signals()
                elif event.type == EventType.MARKET_EOD_DATA:
                    symbol = event.symbol
                    self.broker.close_all_open_positions(symbol)
                elif event.type == EventType.SIGNAL:
                    signal = event.signal
                    self.order_manager.check_signal(signal)
                elif event.type == EventType.MARKET_DATA_END:
                    symbol = event.symbol
                    self.broker.close_all_open_positions(
                        symbol=symbol, end_of_day=False
                    )
                    data_to_process = False
                    break
