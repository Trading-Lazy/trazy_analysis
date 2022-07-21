from typing import Dict, Any

from trazy_analysis.indicators.indicator import CandleIndicator
from trazy_analysis.indicators.indicators_managers import ReactiveIndicators
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.parameter import Discrete
from trazy_analysis.models.signal import Signal
from trazy_analysis.strategy.strategy import LOG, Strategy


class SmaCrossoverStrategy(Strategy):
    DEFAULT_PARAMETERS = {
        "short_sma": 9,
        "long_sma": 65,
    }

    DEFAULT_PARAMETERS_SPACE = {
        "short_sma": Discrete([5, 75]),
        "long_sma": Discrete([100, 200]),
    }

    def __init__(
        self,
        data: CandleIndicator,
        parameters: dict[str, Any],
        indicators: ReactiveIndicators,
    ):
        super().__init__(data, parameters, indicators)
        self.short_sma = self.indicators.Sma(
            self.close, period=self.parameters["short_sma"]
        )
        self.long_sma = self.indicators.Sma(
            self.close, period=self.parameters["long_sma"]
        )
        self.crossover = self.indicators.Crossover(self.short_sma, self.long_sma)

    def current(self, candle: Candle) -> None:
        LOG.info("short_sma = %s", self.short_sma.data)
        LOG.info("short_sma sum = %s", self.short_sma.sum)
        LOG.info("long_sma = %s", self.long_sma.data)
        LOG.info("long_sma sum = %s", self.long_sma.sum)
        LOG.info("crossover = %s", self.crossover.data)
        LOG.info("crossover state = %s", self.crossover.state.name)
        if self.crossover > 0:
            self.add_signal(
                Signal(
                    asset=candle.asset,
                    time_unit=candle.time_unit,
                    action=Action.BUY,
                    direction=Direction.LONG,
                )
            )
        elif self.crossover < 0:
            self.add_signal(
                Signal(
                    asset=candle.asset,
                    time_unit=candle.time_unit,
                    action=Action.SELL,
                    direction=Direction.LONG,
                )
            )
