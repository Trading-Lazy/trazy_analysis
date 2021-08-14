from collections import deque
from datetime import timedelta

import pytest

from trazy_analysis.bot.event_loop import EventLoop
from trazy_analysis.broker.binance_fee_model import BinanceFeeModel
from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.broker.kucoin_fee_model import KucoinFeeModel
from trazy_analysis.broker.simulated_broker import SimulatedBroker
from trazy_analysis.common.clock import SimulatedClock
from trazy_analysis.feed.feed import CsvFeed, Feed
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction, OrderType
from trazy_analysis.models.order import Order
from trazy_analysis.order_manager.order_creator import OrderCreator
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.order_manager.position_sizer import PositionSizer
from trazy_analysis.strategy.strategies.arbitrage_strategy import ArbitrageStrategy

BINANCE_EXCHANGE = "BINANCE"
KUCOIN_EXCHANGE = "KUCOIN"
BINANCE_ASSET = Asset(symbol="XRP/USDT", exchange=BINANCE_EXCHANGE)
KUCOIN_ASSET = Asset(symbol="XRP/USDT", exchange=KUCOIN_EXCHANGE)


def test_arbitrage_strategy():
    events = deque()
    feed: Feed = CsvFeed(
        {
            BINANCE_ASSET: f"test/data/xrpusdt_one_week_binance.csv",
            KUCOIN_ASSET: f"test/data/xrpusdt_one_week_kucoin.csv",
        },
        events,
    )

    # Create 2 simulated brokers for the 2 exchanges
    clock = SimulatedClock()
    initial_budget = 100
    initial_funds = initial_budget / 4

    # Binance simulated broker
    binance_fee_model = BinanceFeeModel()
    binance_broker = SimulatedBroker(
        clock,
        events,
        initial_funds=initial_funds,
        fee_model=binance_fee_model,
        exchange=BINANCE_EXCHANGE,
    )
    binance_broker.subscribe_funds_to_portfolio(initial_funds)
    binance_first_candle = feed.candle_dataframes[BINANCE_ASSET].get_candle(0)
    binance_broker.update_price(binance_first_candle)

    # Kucoin simulated broker
    kucoin_fee_model = KucoinFeeModel()
    kucoin_broker = SimulatedBroker(
        clock,
        events,
        initial_funds=initial_funds,
        fee_model=kucoin_fee_model,
        exchange=KUCOIN_EXCHANGE,
    )
    kucoin_broker.subscribe_funds_to_portfolio(initial_funds)
    kucoin_first_candle = feed.candle_dataframes[KUCOIN_ASSET].get_candle(0)
    kucoin_broker.update_price(kucoin_first_candle)

    # Buy in advance securities to have something to sell for the purpose of the test
    initial_size = 1

    # Binance
    candle = Candle(asset=BINANCE_ASSET, open=0, high=0, low=0, close=0, volume=0)
    binance_broker.update_price(candle)
    order = Order(
        asset=BINANCE_ASSET,
        action=Action.BUY,
        direction=Direction.LONG,
        size=initial_size,
        signal_id="0",
        limit=None,
        stop=None,
        target=None,
        stop_pct=None,
        type=OrderType.MARKET,
        clock=clock,
        time_in_force=timedelta(minutes=5),
    )
    binance_broker.execute_market_order(order)

    # Kucoin
    candle = Candle(asset=KUCOIN_ASSET, open=0, high=0, low=0, close=0, volume=0)
    kucoin_broker.update_price(candle)
    order = Order(
        asset=KUCOIN_ASSET,
        action=Action.BUY,
        direction=Direction.LONG,
        size=initial_size,
        signal_id="0",
        limit=None,
        stop=None,
        target=None,
        stop_pct=None,
        type=OrderType.MARKET,
        clock=clock,
        time_in_force=timedelta(minutes=5),
    )
    kucoin_broker.execute_market_order(order)

    # prepare event loop parameters
    broker_manager = BrokerManager(
        brokers={
            BINANCE_EXCHANGE: binance_broker,
            KUCOIN_EXCHANGE: kucoin_broker,
        },
        clock=clock,
    )
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(
        events=events,
        broker_manager=broker_manager,
        position_sizer=position_sizer,
        order_creator=order_creator,
    )
    indicators_manager = IndicatorsManager(preload=True, initial_data=feed.candles)

    strategies_parameters = {ArbitrageStrategy: [{"margin_factor": 1}]}
    assets = [BINANCE_ASSET, KUCOIN_ASSET]
    event_loop = EventLoop(
        events=events,
        assets=assets,
        feed=feed,
        order_manager=order_manager,
        strategies_parameters=strategies_parameters,
        indicators_manager=indicators_manager,
        close_at_end_of_day=False,
        close_at_end_of_data=False,
    )
    event_loop.loop()

    assert binance_broker.get_portfolio_total_equity() == pytest.approx(
        25.8558, abs=0.0001
    )
    assert kucoin_broker.get_portfolio_total_equity() == pytest.approx(
        25.5385, abs=0.0001
    )
