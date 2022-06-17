from trazy_analysis.common.backtest import Backtest, BacktestConfig
from trazy_analysis.broker.binance_fee_model import BinanceFeeModel
from trazy_analysis.models.asset import Asset
from trazy_analysis.strategy.strategies.smart_money_concept import (
    SmartMoneyConcept,
)
from trazy_analysis.models.enums import OrderType
from trazy_analysis.db_storage.influxdb_storage import InfluxDbStorage
from trazy_analysis.statistics.statistics import Statistics
from datetime import datetime, timedelta
import pytz


def test_smart_money_concept():
    assets = [
        Asset(symbol="BTCUSDT", exchange="BINANCE", time_unit=timedelta(minutes=1)),
        Asset(symbol="BTCUSDT", exchange="BINANCE", time_unit=timedelta(minutes=5))
    ]
    start = datetime(2022, 5, 17, 23, 57, 0, 0, tzinfo=pytz.UTC)
    end = datetime(2022, 5, 25, 0, 0, 0, 0, tzinfo=pytz.UTC)
    db_storage = InfluxDbStorage()

    backtest = Backtest(
        assets=assets,
        fee_model=BinanceFeeModel(),
        start=start,
        end=end,
        download=False,
        initial_funds=10000.0,
        integer_size=False,
        fixed_order_type=OrderType.TARGET,
        target_order_pct=0.03,
        stop_order_pct=0.01,
        with_bracket=True,
        db_storage=db_storage,
        save_db_storage=None,
        preload=False,
        close_at_end_of_day=False,
        statistics_class=Statistics,
    )
    backtest.run_strategy(SmartMoneyConcept, SmartMoneyConcept.DEFAULT_PARAMETERS)

# def test_live():
#     from trazy_analysis.common.live import Live
#     from trazy_analysis.models.asset import Asset
#     from trazy_analysis.strategy.strategies.smart_money_concept import (
#         SmartMoneyConcept,
#     )
#     from trazy_analysis.models.enums import OrderType
#     from trazy_analysis.statistics.statistics import Statistics
#     from datetime import timedelta
#
#     assets = [
#         Asset(symbol="BTCUSDT", exchange="binance", time_unit=timedelta(minutes=1)),
#     ]
#
#     live = Live(
#         assets=assets,
#         integer_size=False,
#         fixed_order_type=OrderType.TARGET,
#         target_order_pct=0.03,
#         stop_order_pct=0.01,
#         with_bracket=True,
#         preload=False,
#         close_at_end_of_day=False,
#         statistics_class=Statistics,
#         simulate=True,
#         simulation_initial_funds=10000.0,
#     )

    #live.run_strategy(SmartMoneyConcept, SmartMoneyConcept.DEFAULT_PARAMETERS)


def test_plot():
    import plotly.graph_objects as go

    feed: Feed = CsvFeed({ASSET: "test/data/btc_usdt_one_day.csv"}, deque())
    db_storage = InfluxDbStorage()
    feed: Feed = ExternalStorageFeed(
        assets=[ASSET],
        start=start,
        end=end,
        events=deque(),
        db_storage=db_storage,
        file_storage=None,
        market_cal=None,
    )
    df = feed.candle_dataframes[ASSET]

    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df.index,
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
            )
        ]
    )

    fig.show()
