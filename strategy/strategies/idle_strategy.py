from typing import Dict

from trazy_analysis.indicators.indicator import CandleIndicator
from trazy_analysis.indicators.indicators_managers import ReactiveIndicators
from trazy_analysis.models.candle import Candle
from trazy_analysis.strategy.strategy import Strategy


class IdleStrategy(Strategy):
    def __init__(
        self,
        data: CandleIndicator,
        parameters: Dict[str, float],
        indicators: ReactiveIndicators,
    ):
        super().__init__(data, parameters, indicators)

    def current(self, candles: Candle) -> None:  # pragma: no cover
        pass
