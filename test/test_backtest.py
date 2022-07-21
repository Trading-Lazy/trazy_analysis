from trazy_analysis.broker.binance_fee_model import BinanceFeeModel
from trazy_analysis.common.backtest import Backtest
from trazy_analysis.models.asset import Asset
from trazy_analysis.strategy.strategies.smart_money_concept import (
    SmartMoneyConcept,
)
from trazy_analysis.models.enums import OrderType, IndicatorMode
from trazy_analysis.db_storage.influxdb_storage import InfluxDbStorage
from trazy_analysis.statistics.statistics import Statistics
from datetime import datetime, timedelta
import pytz

def test_backtest():
    assets = {
        Asset(symbol="BTCUSDT", exchange="BINANCE"): timedelta(minutes=5)
    }
    start = datetime(2022, 6, 8, 0, 0, 0, 0, tzinfo=pytz.UTC)
    end = datetime(2022, 6, 9, 0, 0, 0, 0, tzinfo=pytz.UTC)
    db_storage = InfluxDbStorage()

    backtest = Backtest(
        assets=assets,
        fee_models=BinanceFeeModel(),
        start=start,
        end=end,
        initial_funds=10000.0,
        integer_size=False,
        fixed_order_type=OrderType.TARGET,
        target_order_pct=0.03,
        stop_order_pct=0.01,
        with_bracket=True,
        indicator_mode=IndicatorMode.LIVE,
        db_storage=None,
        store_medium=db_storage,
        close_at_end_of_day=False,
        statistics_class=Statistics,
    )

    backtest.run_strategy(SmartMoneyConcept, SmartMoneyConcept.DEFAULT_PARAMETERS)