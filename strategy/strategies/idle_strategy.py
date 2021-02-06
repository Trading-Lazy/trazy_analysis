from common.clock import Clock
from db_storage.db_storage import DbStorage
from indicators.indicators import IndicatorsManager
from models.candle import Candle
from models.signal import Signal
from order_manager.order_manager import OrderManager
from strategy.strategy import Strategy


class IdleStrategy(Strategy):
    def __init__(
        self,
        symbol: str,
        db_storage: DbStorage,
        order_manager: OrderManager,
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(symbol, db_storage, order_manager, indicators_manager)

    def generate_signal(
        self, candle: Candle, clock: Clock
    ) -> Signal:  # pragma: no cover
        return None
