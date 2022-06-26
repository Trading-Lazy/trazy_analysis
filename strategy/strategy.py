import abc
import os
from collections import deque
from datetime import timedelta
from typing import Dict, List, Set, Union, Any

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.common.clock import Clock
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.event import SignalEvent
from trazy_analysis.models.parameter import Parameter
from trazy_analysis.models.signal import SignalBase
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.strategy.context import Context

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class StrategyBase:
    __metaclass__ = abc.ABCMeta

    @classmethod
    @abc.abstractmethod
    def DEFAULT_PARAMETERS(cls) -> Dict[str, float]:  # pragma: no cover
        return {}

    @classmethod
    @abc.abstractmethod
    def DEFAULT_PARAMETERS_SPACE(cls) -> Dict[str, Parameter]:  # pragma: no cover
        return {}

    def __init__(
        self,
        order_manager: OrderManager,
        events: deque,
        parameters: Dict[str, Parameter],
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__()
        self.order_manager = order_manager
        self.events = events
        self.parameters = parameters
        self.indicators_manager = indicators_manager
        self.signals: List[SignalBase] = []
        self.name = self.__class__.__name__

    def add_signal(self, signal: SignalBase):
        self.signals.append(signal)

    def add_signals(self, signals: List[SignalBase]):
        self.signals.extend(signals)

    @abc.abstractmethod
    def current(
        self, candles: Union[Candle, List[Candle]], clock: Clock
    ) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def process_context(self, context: Context, clock: Clock) -> None:
        raise NotImplementedError


class Strategy(StrategyBase):
    __metaclass__ = abc.ABCMeta

    def __init__(
        self,
        asset: Asset,
        time_unit: timedelta,
        order_manager: OrderManager,
        events: deque,
        parameters: Dict[str, Parameter],
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(order_manager, events, parameters, indicators_manager)
        self.asset = asset
        self.time_unit = time_unit

    @abc.abstractmethod
    def current(self, candle: Candle, clock: Clock) -> None:  # pragma: no cover
        raise NotImplementedError

    def process_context(self, context: Context, clock: Clock) -> None:
        self.signals = []
        candle = context.get_last_candle(self.asset, self.time_unit)
        if candle is not None:
            self.current(
                context.get_last_candle(self.asset, self.time_unit),
                clock,
            )
            self.events.append(SignalEvent(self.signals))


class MultiAssetsStrategy(StrategyBase):
    __metaclass__ = abc.ABCMeta

    def __init__(
        self,
        assets: Dict[Asset, List[timedelta]],
        order_manager: OrderManager,
        events: deque,
        parameters: Dict[str, Parameter],
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(order_manager, events, parameters, indicators_manager)
        self.assets: Dict[Asset, Set[timedelta]] = {
            asset: set(assets[asset]) for asset in assets
        }

    @abc.abstractmethod
    def current(
        self, candles: List[Candle], clock: Clock
    ) -> List[SignalBase]:  # pragma: no cover
        raise NotImplementedError

    def process_context(self, context: Context, clock: Clock) -> None:
        self.signals = []
        last_candles = context.get_last_candles()
        last_candles = [
            candle
            for candle in last_candles
            if candle.asset in self.assets
            and candle.time_unit in self.assets[candle.asset]
        ]
        self.current(last_candles, clock)
        self.events.append(SignalEvent(self.signals))


class StrategyConfig:
    def __init__(
        self,
        strategy_class: type,
        assets: Dict[Asset, List[timedelta]],
        parameters: Dict[str, Any],
    ):
        self.strategy_class = strategy_class
        self.assets = assets
        self.parameters = parameters
