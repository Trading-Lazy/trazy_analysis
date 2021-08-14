from collections import deque
from typing import Dict, List

from trazy_analysis.common.clock import Clock
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.signal import ArbitragePairSignal, Signal
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.strategy.context import Context
from trazy_analysis.strategy.strategy import LOG, Strategy


class ArbitrageStrategy(Strategy):
    DEFAULT_PARAMETERS = {"margin_factor": 2}

    def __init__(
        self,
        context: Context,
        order_manager: OrderManager,
        events: deque,
        parameters: Dict[str, float],
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(context, order_manager, events, parameters, indicators_manager)
        self.commission = 0.001

    def generate_signals(
        self, context: Context, clock: Clock
    ) -> List[Signal]:  # pragma: no cover
        signals = []
        candles = context.get_last_candles()
        margin_factor = self.parameters["margin_factor"]
        for candle1 in candles:
            for candle2 in candles:
                if (
                    candle1.asset.exchange == candle2.asset.exchange
                    or candle1.asset.symbol != candle2.asset.symbol
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
                        root_candle_timestamp=context.current_timestamp,
                        parameters={},
                        clock=clock,
                    )
                    signal2 = Signal(
                        action=action2,
                        direction=Direction.LONG,
                        confidence_level=1.0,
                        strategy=self.name,
                        asset=candle2.asset,
                        root_candle_timestamp=context.current_timestamp,
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
                    signals.append(arbitrage_pair_signal)
            return signals
