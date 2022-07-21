import abc
import os
from typing import Dict, List, Union, Any

import telegram_send

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.common.clock import Clock
from trazy_analysis.indicators.common import PriceType
from trazy_analysis.indicators.indicator import CandleIndicator, CandleData
from trazy_analysis.indicators.indicators_managers import ReactiveIndicators
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.event import SignalEvent
from trazy_analysis.models.parameter import Parameter
from trazy_analysis.models.signal import SignalBase
from trazy_analysis.strategy.context import Context

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


# > This class is a base class for all strategies
class StrategyBase:
    __metaclass__ = abc.ABCMeta

    @classmethod
    @abc.abstractmethod
    def DEFAULT_PARAMETERS(cls) -> dict[str, Any]:  # pragma: no cover
        return {}

    @classmethod
    @abc.abstractmethod
    def DEFAULT_PARAMETERS_SPACE(cls) -> dict[str, Parameter]:  # pragma: no cover
        return {}

    def __init__(
        self,
        data: CandleIndicator | CandleData,
        parameters: dict[str, Any],
        indicators: ReactiveIndicators,
    ):
        super().__init__()
        self.context: Context = None
        self.data = data
        self.parameters = parameters
        self.indicators = indicators
        self.signals: list[SignalBase] = []
        self.name = self.__class__.__name__

    def set_context(self, context: Context):
        self.context = context

    def add_signal(self, signal: SignalBase):
        self.signals.append(signal)

    def add_signals(self, signals: list[SignalBase]):
        self.signals.extend(signals)

    @abc.abstractmethod
    def current(self, candles: Candle | list[Candle]) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def process_context(self, context: Context, clock: Clock) -> None:
        raise NotImplementedError


# > This class is a strategy that can trade a single asset
class Strategy(StrategyBase):
    __metaclass__ = abc.ABCMeta

    def __init__(
        self,
        data: CandleIndicator,
        parameters: dict[str, Any],
        indicators: ReactiveIndicators,
    ):
        super().__init__(data, parameters, indicators)
        self.open = self.data(PriceType.OPEN)
        self.high = self.data(PriceType.HIGH)
        self.low = self.data(PriceType.LOW)
        self.close = self.data(PriceType.CLOSE)
        self.asset = data.asset
        self.time_unit = data.time_unit

    @abc.abstractmethod
    def current(self, candle: Candle) -> None:  # pragma: no cover
        raise NotImplementedError

    def process_context(self, context: Context, clock: Clock) -> None:
        self.signals = []
        candle = context.get_last_candle(self.asset, self.time_unit)
        if candle is not None:
            self.current(candle)
            for signal in self.signals:
                signal.enrich_signal(
                    confidence_level=1.0,
                    strategy=self.name,
                    root_candle_timestamp=candle.timestamp,
                    clock=clock,
                    parameters=self.parameters,
                )
            if self.signals:
                self.context.add_event(SignalEvent(self.signals))

    @staticmethod
    def send_notification(*messages):
        telegram_send.send(messages=list(messages))


# It's a strategy that can trade multiple assets
class MultiAssetsStrategy(StrategyBase):
    __metaclass__ = abc.ABCMeta

    def __init__(
        self,
        data: CandleData,
        parameters: dict[str, Any],
        indicators: ReactiveIndicators,
    ):
        super().__init__(data, parameters, indicators)

    @abc.abstractmethod
    def current(self, candles: list[Candle]) -> list[SignalBase]:  # pragma: no cover
        raise NotImplementedError

    def process_context(self, context: Context, clock: Clock) -> None:
        self.signals = []
        last_candles = context.get_last_candles()
        last_candles = [
            candle
            for candle in last_candles
            if self.data.exists(candle.asset, candle.time_unit)
        ]
        self.current(last_candles)
        if self.signals:
            self.context.add_event(SignalEvent(self.signals))
