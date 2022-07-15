from collections import deque
from typing import List, Dict, Any

import ccxt
import pandas as pd
from pandas_market_calendars import MarketCalendar

from trazy_analysis.bot.event_loop import EventLoop
from trazy_analysis.broker.binance_fee_model import BinanceFeeModel
from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.broker.ccxt_broker import CcxtBroker
from trazy_analysis.broker.degiro_broker import DegiroBroker
from trazy_analysis.broker.fee_model import FeeModel
from trazy_analysis.broker.simulated_broker import SimulatedBroker
from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.clock import LiveClock
from trazy_analysis.common.constants import NONE_API_KEYS
from trazy_analysis.common.crypto_exchange_calendar import CryptoExchangeCalendar
from trazy_analysis.feed.feed import (
    LiveFeed,
)
from trazy_analysis.market_data.live.ccxt_live_data_handler import CcxtLiveDataHandler
from trazy_analysis.market_data.live.tiingo_live_data_handler import (
    TiingoLiveDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import OrderType, BrokerIsolation
from trazy_analysis.order_manager.order_creator import OrderCreator
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.order_manager.position_sizer import PositionSizer
from trazy_analysis.statistics.statistics import Statistics


class LiveConfig:
    def __init__(
        self,
        assets: List[Asset],
        market_cal: MarketCalendar = CryptoExchangeCalendar(),
        integer_size: bool = False,
        fixed_order_type: OrderType = OrderType.MARKET,
        limit_order_pct=0.005,
        stop_order_pct=0.05,
        target_order_pct=0.01,
        trailing_stop_order_pct=0.05,
        with_cover=False,
        with_bracket=False,
        with_trailing_cover=False,
        with_trailing_bracket=False,
        exchanges_api_keys: Dict[str, str] = {},
        preload: bool = True,
        close_at_end_of_day=True,
        isolation: BrokerIsolation = BrokerIsolation.EXCHANGE,
        statistics_class: type = Statistics,
        events: deque = deque(),
    ):
        self.assets = assets
        self.market_cal = market_cal
        self.integer_size = integer_size
        self.fixed_order_type = fixed_order_type
        self.limit_order_pct = limit_order_pct
        self.stop_order_pct = stop_order_pct
        self.target_order_pct = target_order_pct
        self.trailing_stop_order_pct = trailing_stop_order_pct
        self.with_cover = with_cover
        self.with_bracket = with_bracket
        self.with_trailing_cover = with_trailing_cover
        self.with_trailing_bracket = with_trailing_bracket
        self.exchanges_api_keys = exchanges_api_keys
        self.preload = preload
        self.close_at_end_of_day = close_at_end_of_day
        self.isolation = isolation
        self.statistics_class = statistics_class

        self.events = events
        self.feed = None

        self.exchanges = [asset.exchange.lower() for asset in assets]
        live_data_handlers = {}
        exchanges_api_keys = {
            exchange.lower(): api_key
            for exchange, api_key in exchanges_api_keys.items()
        }

        # Crypto currency exchanges
        ccxt_exchanges_api_keys = {}
        self.ccxt_exchanges = ccxt.exchanges
        self.other_exchanges = []
        for exchange in self.exchanges:
            if exchange in self.ccxt_exchanges:
                if exchange in exchanges_api_keys:
                    ccxt_exchanges_api_keys[exchange] = exchanges_api_keys[exchange]
                else:
                    ccxt_exchanges_api_keys[exchange] = NONE_API_KEYS
            else:
                self.other_exchanges.append(exchange)
        if len(ccxt_exchanges_api_keys) != 0:
            self.ccxt_connector = CcxtConnector(
                exchanges_api_keys=ccxt_exchanges_api_keys
            )
            ccxt_live_data_handler = CcxtLiveDataHandler(self.ccxt_connector)
            for exchange in ccxt_exchanges_api_keys:
                live_data_handlers[exchange] = ccxt_live_data_handler

        # Other exchanges
        for exchange in self.other_exchanges:
            if exchange == "iex":
                live_data_handlers["iex"] = TiingoLiveDataHandler()
            else:
                all_exchanges = self.ccxt_exchanges
                all_exchanges.append("iex")
                raise Exception(
                    f"Exchange {exchange} is not supported for now. The list of supported exchanges is: {all_exchanges}"
                )

        self.feed = LiveFeed(
            assets=assets,
            live_data_handlers=live_data_handlers,
            events=self.events,
        )


class Live:
    def __init__(
        self,
        assets: List[Asset],
        market_cal: MarketCalendar = CryptoExchangeCalendar(),
        integer_size: bool = False,
        fixed_order_type: OrderType = OrderType.MARKET,
        limit_order_pct=0.005,
        stop_order_pct=0.05,
        target_order_pct=0.01,
        trailing_stop_order_pct=0.05,
        with_cover=False,
        with_bracket=False,
        with_trailing_cover=False,
        with_trailing_bracket=False,
        exchanges_api_keys: Dict[str, str] = {},
        preload: bool = True,
        close_at_end_of_day=True,
        isolation: BrokerIsolation = BrokerIsolation.EXCHANGE,
        statistics_class: type = Statistics,
        live_config: LiveConfig = None,
        base_currency="EUR",
        supported_currencies=("EUR", "USDT"),
        simulate=False,
        simulation_initial_funds: float = 10000.0,
        simulation_fee_model: FeeModel = BinanceFeeModel(),
    ):
        self.events = deque()
        if live_config is None:
            live_config = LiveConfig(
                assets=assets,
                market_cal=market_cal,
                integer_size=integer_size,
                fixed_order_type=fixed_order_type,
                limit_order_pct=limit_order_pct,
                stop_order_pct=stop_order_pct,
                target_order_pct=target_order_pct,
                trailing_stop_order_pct=trailing_stop_order_pct,
                with_cover=with_cover,
                with_bracket=with_bracket,
                with_trailing_cover=with_trailing_cover,
                with_trailing_bracket=with_trailing_bracket,
                exchanges_api_keys=exchanges_api_keys,
                preload=preload,
                close_at_end_of_day=close_at_end_of_day,
                isolation=isolation,
                statistics_class=statistics_class,
                events=self.events,
            )
        else:
            live_config.events = self.events

        self.live_config = live_config
        self.exchanges = [asset.exchange for asset in self.live_config.assets]
        self.base_currency = base_currency
        self.supported_currencies = supported_currencies
        self.simulate = simulate
        self.simulation_initial_funds = simulation_initial_funds
        self.simulation_fee_model = simulation_fee_model
        self.event_loop = None

    def run_strategies(
        self,
        strategies_parameters: Dict[type, Dict[str, Any]],
    ) -> pd.DataFrame:
        self.events = deque()
        self.live_config.feed.events = self.events
        self.live_config.feed.reset()
        clock = LiveClock(market_cal=self.live_config.market_cal)
        brokers = {}

        if self.simulate:
            for exchange in self.live_config.exchanges:
                brokers[exchange] = SimulatedBroker(
                    clock,
                    self.events,
                    initial_funds=self.simulation_initial_funds,
                    fee_model=self.simulation_fee_model,
                )
                brokers[exchange].subscribe_funds_to_portfolio(
                    self.simulation_initial_funds
                )
        else:
            # Crypto currency exchanges
            for exchange in self.live_config.ccxt_exchanges:
                brokers[exchange] = CcxtBroker(
                    clock=clock,
                    events=self.events,
                    ccxt_connector=self.live_config.ccxt_connector,
                    base_currency=self.base_currency,
                    supported_currencies=self.supported_currencies,
                    execute_at_end_of_day=self.live_config.close_at_end_of_day,
                )
            # Other exchanges
            for exchange in self.live_config.other_exchanges:
                if exchange == "iex":
                    brokers["iex"] = DegiroBroker(
                        clock=clock,
                        events=self.events,
                        base_currency=self.base_currency,
                        supported_currencies=self.supported_currencies,
                    )
                else:
                    all_exchanges = self.live_config.ccxt_exchanges
                    all_exchanges.append("iex")
                    raise Exception(
                        f"Exchange {exchange} is not supported for now. "
                        f"The list of supported exchanges is: {all_exchanges}"
                    )
        broker_manager = BrokerManager(brokers=brokers)
        position_sizer = PositionSizer(
            broker_manager=broker_manager,
            integer_size=self.live_config.integer_size,
        )
        order_creator = OrderCreator(
            broker_manager=broker_manager,
            fixed_order_type=self.live_config.fixed_order_type,
            limit_order_pct=self.live_config.limit_order_pct,
            stop_order_pct=self.live_config.stop_order_pct,
            target_order_pct=self.live_config.target_order_pct,
            trailing_stop_order_pct=self.live_config.trailing_stop_order_pct,
            with_cover=self.live_config.with_cover,
            with_bracket=self.live_config.with_bracket,
            with_trailing_cover=self.live_config.with_trailing_cover,
            with_trailing_bracket=self.live_config.with_trailing_bracket,
        )
        order_manager = OrderManager(
            events=self.events,
            broker_manager=broker_manager,
            position_sizer=position_sizer,
            order_creator=order_creator,
            clock=clock,
        )
        self.event_loop = EventLoop(
            events=self.events,
            assets=self.live_config.assets,
            feed=self.live_config.feed,
            order_manager=order_manager,
            strategies_parameters=strategies_parameters,
            close_at_end_of_day=self.live_config.close_at_end_of_day,
            close_at_end_of_data=False,
            broker_isolation=self.live_config.isolation,
            statistics_class=self.live_config.statistics_class,
        )
        self.event_loop.loop()
        return self.event_loop.statistics_df

    def run_strategy(self, strategy: type, strategy_parameters: Dict[str, Any]) -> None:
        self.run_strategies({strategy: strategy_parameters})
