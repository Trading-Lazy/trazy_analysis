from typing import Dict, List

from trazy_analysis.indicators.indicator import CandleData
from trazy_analysis.indicators.indicators_managers import ReactiveIndicators
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.parameter import Discrete
from trazy_analysis.models.signal import ArbitragePairSignal, Signal
from trazy_analysis.strategy.strategy import LOG, MultiAssetsStrategy


class ArbitrageStrategy(MultiAssetsStrategy):
    DEFAULT_PARAMETERS = {"margin_factor": 2}

    DEFAULT_PARAMETERS_SPACE = {"margin_factor": Discrete([1, 4])}

    def __init__(
        self,
        data: CandleData,
        parameters: Dict[str, float],
        indicators: ReactiveIndicators,
    ):
        super().__init__(data, parameters, indicators)
        self.commission = 0.001

    def current(self, candles: List[Candle]) -> None:  # pragma: no cover
        margin_factor = self.parameters["margin_factor"]
        for i in range(0, len(candles)):
            candle1 = candles[i]
            for j in range(i + 1, len(candles)):
                candle2 = candles[j]
                if (
                    candle1.asset.exchange == candle2.asset.exchange
                    or candle1.asset.symbol != candle2.asset.symbol
                    or candle1.time_unit != candle2.time_unit
                ):
                    continue
                if candle1.volume == 0 or candle2.volume == 0:
                    continue
                diff = abs(candle1.close - candle2.close)
                broker1 = self.context.broker_manager.get_broker(candle1.asset.exchange)
                commission1 = broker1.fee_models[candle1.asset].commission_pct
                broker2 = self.context.broker_manager.get_broker(candle2.asset.exchange)
                commission2 = broker2.fee_models[candle2.asset].commission_pct
                fee1 = candle1.close * commission1
                fee2 = candle2.close * commission2
                total_fee = fee1 + fee2
                LOG.info(f"diff = {diff}")
                LOG.info(f"total fee = {total_fee}")
                if diff > margin_factor * total_fee:
                    LOG.info("There is 1 opportunity")
                    candle1_is_greater = candle1.close > candle2.close
                    action1 = Action.SELL if candle1_is_greater else Action.BUY
                    action2 = Action.BUY if candle1_is_greater else Action.SELL
                    signal1 = Signal(
                        asset=candle1.asset,
                        time_unit=candle1.time_unit,
                        action=action1,
                        direction=Direction.LONG,
                    )
                    signal2 = Signal(
                        asset=candle2.asset,
                        time_unit=candle2.time_unit,
                        action=action2,
                        direction=Direction.LONG,
                    )
                    signal1_is_buy_signal = signal1.action == Action.BUY
                    buy_signal = signal1 if signal1_is_buy_signal else signal2
                    sell_signal = signal2 if signal1_is_buy_signal else signal1
                    arbitrage_pair_signal = ArbitragePairSignal(
                        buy_signal=buy_signal,
                        sell_signal=sell_signal,
                        strategy=self.name,
                        parameters={},
                    )
                    self.add_signal(arbitrage_pair_signal)
