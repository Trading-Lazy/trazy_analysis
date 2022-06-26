from collections import deque
from datetime import timedelta
from typing import Dict, List

from trazy_analysis.common.clock import Clock
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.parameter import Discrete
from trazy_analysis.models.signal import ArbitragePairSignal, Signal
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.strategy.strategy import LOG, MultiAssetsStrategy


class ArbitrageStrategy(MultiAssetsStrategy):
    DEFAULT_PARAMETERS = {"margin_factor": 2}

    DEFAULT_PARAMETERS_SPACE = {"margin_factor": Discrete([1, 4])}

    def __init__(
        self,
        assets: Dict[Asset, List[timedelta]],
        order_manager: OrderManager,
        events: deque,
        parameters: Dict[str, float],
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(assets, order_manager, events, parameters, indicators_manager)
        self.commission = 0.001

    def current(
        self, candles: List[Candle], clock: Clock
    ) -> None:  # pragma: no cover
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
                broker1 = self.order_manager.broker_manager.get_broker(
                    candle1.asset.exchange
                )
                commission1 = broker1.fee_model.commission_pct
                broker2 = self.order_manager.broker_manager.get_broker(
                    candle2.asset.exchange
                )
                commission2 = broker2.fee_model.commission_pct
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
                        action=action1,
                        direction=Direction.LONG,
                        confidence_level=1.0,
                        strategy=self.name,
                        asset=candle1.asset,
                        root_candle_timestamp=candle1.timestamp,
                        parameters={},
                        clock=clock,
                    )
                    signal2 = Signal(
                        action=action2,
                        direction=Direction.LONG,
                        confidence_level=1.0,
                        strategy=self.name,
                        asset=candle2.asset,
                        root_candle_timestamp=candle2.timestamp,
                        parameters={},
                        clock=clock,
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
