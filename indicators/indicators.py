from indicators.rolling_window import (
    PriceRollingWindowFactory,
    RollingWindowFactory,
    TimeFramedCandleRollingWindowFactory,
)
from indicators.sma import SmaFactory

RollingWindow = RollingWindowFactory()
TimeFramedCandleRollingWindow = TimeFramedCandleRollingWindowFactory()
PriceRollingWindow = PriceRollingWindowFactory()
Sma = SmaFactory()
