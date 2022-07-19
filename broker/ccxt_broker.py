import os
import traceback
from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List

import pandas as pd

import trazy_analysis.settings
from trazy_analysis.broker.common import get_rejected_order_error_message
from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.clock import Clock
from trazy_analysis.common.constants import CONNECTION_ERROR_MESSAGE
from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.logger import logger
from trazy_analysis.market_data.common import datetime_from_epoch
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction, OrderType
from trazy_analysis.models.order import Order
from trazy_analysis.portfolio.portfolio import Portfolio
from trazy_analysis.portfolio.portfolio_event import PortfolioEvent
from trazy_analysis.position.position import Position
from trazy_analysis.position.transaction import Transaction

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class CcxtBroker:
    # Every period the positions of the broker get updated
    UPDATE_PRICE_PERIOD = pd.Timedelta(value=10, unit="seconds")
    UPDATE_LOT_SIZE_INFO = timedelta(days=1)
    UPDATE_BALANCES_PERIOD = pd.Timedelta(value=10, unit="seconds")
    UPDATE_TRANSACTIONS_PERIOD = pd.Timedelta(value=10, unit="seconds")
    PRODUCT_INFO_PERIOD = pd.Timedelta(value=10, unit="days")
    SYMBOL_INFO_PERIOD = pd.Timedelta(value=10, unit="days")
    TRANSACTION_LOOKBACK_PERIOD = timedelta(minutes=10)
    CURRENCY_MAPPING = {"EUR": "EUR", "USD": "USD"}

    def __init__(
        self,
        clock: Clock,
        events: deque,
        ccxt_connector: CcxtConnector,
        base_currency: str = "EUR",
        supported_currencies: List[str] = ["EUR", "USDT"],
        execute_at_end_of_day=True,
    ):
        self.supported_currencies = supported_currencies
        self.execute_at_end_of_day = execute_at_end_of_day
        self.base_currency = self._set_base_currency(base_currency)
        self.clock = clock
        self.events = events
        self.ccxt_connector = ccxt_connector
        self.cash_balances = {
            exchange.lower(): {currency: 0 for currency in self.supported_currencies}
            for exchange in self.ccxt_connector.exchanges
        }
        self.open_orders = self._set_initial_open_orders()
        self.open_orders_bars_delay = 0
        self.exit_orders = self._set_initial_exit_orders()
        self.last_prices = self._set_initial_last_prices()

        # create portfolio
        self._create_initial_portfolio()

        self.price_last_update = None
        self.currency_pairs_traded = set()
        self.lot_size = {}
        self.lot_size_last_update = None
        self.update_lot_size_info()
        self.update_price()
        self.balances_last_update = None
        self.update_balances_and_positions()
        self.transactions_last_update: datetime = self.clock.current_time()
        self.open_orders_ids = {
            exchange.lower(): set() for exchange in self.ccxt_connector.exchanges
        }
        self.update_transactions()

    def _set_base_currency(self, base_currency: str) -> str:
        """
        Check and set the base currency from a list of
        allowed currencies. Raise ValueError if the
        currency is currently not supported by QSTrader.
        Parameters
        ----------
        base_currency : `str`
            The base currency string.
        Returns
        -------
        `str`
            The base currency string.
        """
        if base_currency not in self.supported_currencies:
            raise ValueError(
                "Currency '%s' is not supported. Could not "
                "set the base currency in the SimulatedBroker "
                "entity." % base_currency
            )
        else:
            return base_currency

    def _set_initial_open_orders(self) -> deque:
        """
        Set the appropriate initial open orders dictionary.
        Returns
        -------
        `queue`
            The empty initial open orders dictionary.
        """
        return deque()

    def _set_initial_exit_orders(self) -> dict:
        """
        Set the appropriate initial exit orders dictionary.
        Returns
        -------
        `dict`
            The empty initial open orders dictionary.
        """
        return {}

    def _set_initial_last_prices(self) -> dict:
        """
        Set the appropriate initial last prices dictionary.
        Returns
        -------
        `dict`
            The empty initial last prices dictionary.
        """
        return {}

    def _create_initial_portfolio(self) -> str:
        """
        Create a new portfolio
        Parameters
        """
        p = Portfolio(currency=self.base_currency)
        self.portfolio = p
        LOG.info(
            "(%s) - portfolio creation: Portfolio created", self.clock.current_time()
        )

    def update_lot_size_info(self) -> None:
        now = self.clock.current_time()
        if (
            self.lot_size_last_update is not None
            and now - self.lot_size_last_update < CcxtBroker.UPDATE_LOT_SIZE_INFO
        ):
            return

        for exchange in self.ccxt_connector.exchanges:
            try:
                exchange_to_lower = exchange.lower()
                exchange_instance = self.ccxt_connector.get_exchange_instance(
                    exchange_to_lower
                )
                symbols_dict = exchange_instance.fetchMarkets()
            except Exception as e:
                LOG.warning(
                    CONNECTION_ERROR_MESSAGE,
                    str(e),
                    traceback.format_exc(),
                )
                return
            for symbol_dict in symbols_dict:
                symbol_info = symbol_dict["info"]
                parser = self.ccxt_connector.get_parser(exchange_to_lower)
                symbol, lot_size = parser.parse_lot_size_info(symbol_info)
                asset = Asset(symbol=symbol, exchange=exchange_to_lower)
                self.lot_size[asset] = lot_size
        self.lot_size_last_update = self.clock.current_time()

    def update_price(self, candle: Candle = None) -> None:
        now = self.clock.current_time()
        if (
            self.price_last_update is not None
            and now - self.price_last_update < CcxtBroker.UPDATE_PRICE_PERIOD
        ):
            return
        for exchange in self.ccxt_connector.exchanges:
            try:
                exchange_to_lower = exchange.lower()
                exchange_instance = self.ccxt_connector.get_exchange_instance(
                    exchange_to_lower
                )
                tickers_info = exchange_instance.fetch_tickers()
            except Exception as e:
                LOG.warning(
                    CONNECTION_ERROR_MESSAGE,
                    str(e),
                    traceback.format_exc(),
                )
                return
            for raw_symbol, price_dict in tickers_info.items():
                parser = self.ccxt_connector.get_parser(exchange_to_lower)
                price_info = price_dict["info"]
                symbol, price = parser.parse_price_info(price_info)
                asset = Asset(symbol=symbol, exchange=exchange_to_lower)
                self.last_prices[asset] = price
                self.portfolio.update_market_value_of_symbol(asset, price, now)
            self.price_last_update = self.clock.current_time()

    def update_balances(self) -> Dict[str, Dict[str, float]]:
        open_balances = {}
        total_cash = 0
        for exchange in self.ccxt_connector.exchanges:
            try:
                exchange_to_lower = exchange.lower()
                exchange_instance = self.ccxt_connector.get_exchange_instance(
                    exchange_to_lower
                )
                balances_dict = exchange_instance.fetchBalance()
            except Exception as e:
                LOG.warning(
                    CONNECTION_ERROR_MESSAGE,
                    str(e),
                    traceback.format_exc(),
                )
                return {}
            parser = self.ccxt_connector.get_parser(exchange_to_lower)
            balances_info = balances_dict["info"]
            open_balances[exchange_to_lower] = parser.parse_balances_info(balances_info)

            # cash balances
            for currency in self.supported_currencies:
                if currency not in open_balances[exchange_to_lower]:
                    continue
                self.cash_balances[exchange_to_lower][currency] = open_balances[
                    exchange_to_lower
                ][currency]
                del open_balances[exchange_to_lower][currency]

            total_cash += self.cash_balances[exchange_to_lower][self.base_currency]
        self.portfolio.cash = total_cash
        return open_balances

    def update_positions(self, open_balances: Dict[str, float]) -> None:
        # open positions
        portfolio = self.portfolio
        positions = portfolio.pos_handler.positions
        for exchange in open_balances:
            for symbol in open_balances[exchange]:
                size = open_balances[exchange][symbol]
                if size > 0:
                    direction = Direction.LONG
                    buy_size = size
                    sell_size = 0
                else:
                    direction = Direction.SHORT
                    buy_size = 0
                    sell_size = size

                currency_pair = Asset(
                    symbol=symbol + "/" + self.base_currency, exchange=exchange
                )
                currency_pair_reversed = Asset(
                    symbol=self.base_currency + "/" + symbol, exchange=exchange
                )
                if currency_pair not in self.lot_size:
                    continue

                last_price = (
                    self.last_prices[currency_pair]
                    if currency_pair in self.last_prices
                    else 1 / self.last_prices[currency_pair_reversed]
                )

                get_or_create_nested_dict(positions, currency_pair, direction)
                positions[currency_pair][direction] = Position(
                    currency_pair,
                    last_price,
                    buy_size,
                    sell_size,
                    direction,
                )
                self.currency_pairs_traded.add(currency_pair)

    def update_balances_and_positions(self) -> None:
        now = self.clock.current_time()
        if (
            self.balances_last_update is not None
            and now - self.balances_last_update < CcxtBroker.UPDATE_BALANCES_PERIOD
        ):
            return

        open_balances = self.update_balances()

        self.update_positions(open_balances)

        self.balances_last_update = self.clock.current_time()

        LOG.info("Balances and positions have been updated")

    def has_opened_position(self, asset: Asset, direction: Direction) -> bool:
        self.update_balances_and_positions()
        portfolio = self.portfolio
        positions = portfolio.pos_handler.positions
        if asset not in positions or direction not in positions[asset]:
            return False
        position = positions[asset][direction]
        return position.net_size >= self.lot_size[asset]

    def subscribe_funds_to_account(self, amount: float) -> None:  # pragma: no cover
        # TODO automate bank transfers to degiro account
        pass

    def withdraw_funds_from_account(self, amount: float) -> None:  # pragma: no cover
        # TODO automate bank transfers to degiro account
        pass

    def update_transactions(self) -> None:
        now = self.clock.current_time()
        if (
            self.transactions_last_update is not None
            and now - self.transactions_last_update
            < CcxtBroker.UPDATE_TRANSACTIONS_PERIOD
        ):
            return
        # get confirmed orders that are opened
        epoch_ms = int(self.transactions_last_update.timestamp()) * 1000
        for exchange in self.ccxt_connector.exchanges:
            exchange_to_lower = exchange.lower()
            exchange_instance = self.ccxt_connector.get_exchange_instance(
                exchange_to_lower
            )
            for currency_pair in self.currency_pairs_traded:
                try:
                    trades_dict = exchange_instance.fetchMyTrades(
                        symbol=currency_pair.symbol,
                        since=epoch_ms,
                    )
                except Exception as e:
                    LOG.warning(
                        CONNECTION_ERROR_MESSAGE,
                        str(e),
                        traceback.format_exc(),
                    )
                    return
                for trade_dict in trades_dict:
                    parser = self.ccxt_connector.get_parser(exchange_to_lower)
                    trade_info = trade_dict["info"]
                    (
                        trade_epoch_ms,
                        symbol,
                        size,
                        action,
                        price,
                        order_id,
                        commission,
                        transaction_id,
                    ) = parser.parse_trade_info(trade_info)
                    if trade_epoch_ms < epoch_ms:
                        continue
                    timestamp = datetime_from_epoch(trade_epoch_ms)
                    asset = Asset(symbol=symbol, exchange=exchange)
                    direction = Direction.LONG if size > 0.0 else Direction.SHORT
                    transaction = Transaction(
                        asset=asset,
                        size=size,
                        action=action,
                        direction=direction,
                        price=price,
                        order_id=order_id,
                        commission=commission,
                        timestamp=timestamp,
                        transaction_id=transaction_id,
                    )
                    description = "%s %s %s %s %s %s" % (
                        action.name,
                        direction.name,
                        transaction.size,
                        transaction.asset.key().upper(),
                        transaction.price,
                        datetime.strftime(transaction.timestamp, "%d/%m/%Y"),
                    )
                    if transaction.action == Action.BUY:
                        pe = PortfolioEvent(
                            timestamp=transaction.timestamp,
                            type="symbol_transaction",
                            description=description,
                            debit=transaction.cost_with_commission,
                            credit=0.0,
                            balance=self.portfolio.cash,
                        )
                    else:
                        pe = PortfolioEvent(
                            timestamp=transaction.timestamp,
                            type="symbol_transaction",
                            description=description,
                            debit=0.0,
                            credit=-1.0 * round(transaction.cost_with_commission, 2),
                            balance=round(self.portfolio.cash, 2),
                        )
                    self.portfolio.history.append(pe)

        LOG.info("Transactions have been synchronized")
        self.transactions_last_update = self.clock.current_time()

    def get_cash_balance(self, currency: str = None) -> float:
        self.update_balances_and_positions()
        if currency is None:
            return self.cash_balances

        found = False
        cash_balance = 0
        for exchange in self.cash_balances:
            if currency in self.cash_balances[exchange]:
                found = True
                cash_balance += self.cash_balances[exchange][currency]
        if not found:
            raise ValueError(
                "Currency of type '%s' is not found within the "
                "broker cash master accounts. Could not retrieve "
                "cash balance." % currency
            )
        return cash_balance

    def max_entry_order_size(
        self, asset: Asset, direction: Direction, cash: float = None
    ) -> int:
        if cash is None:
            cash = self.cash_balances[asset.exchange][self.base_currency]
        price = self.current_price(asset)
        fee_model = self.ccxt_connector.get_fee_model(asset.exchange.lower())
        return fee_model.calc_max_size_for_cash(cash=cash, price=price)

    def position_size(self, asset: Asset, direction: Direction) -> int:
        return super().position_size(asset, direction)

    def symbol_mapping(self, symbol: str) -> str:
        for currency in CcxtBroker.CURRENCY_MAPPING:
            mapping = CcxtBroker.CURRENCY_MAPPING[currency]
            if symbol.endswith(currency):
                return symbol.replace(currency, mapping)

    def truncate_order_size(self, order: Order) -> float:
        min_lot_size = self.lot_size[order.asset]
        decimal_size = Decimal(str(order.size))
        decimal_min_lot_size = Decimal(str(min_lot_size))
        truncated_size = decimal_size.quantize(
            decimal_min_lot_size, rounding=ROUND_DOWN
        )
        return truncated_size

    def execute_market_order(self, order: Order) -> None:
        try:
            LOG.info("Submit market order to exchange")
            if order.is_exit_order and self.has_opened_position(
                order.asset, order.direction
            ):
                self.update_balances_and_positions()
                available_size = self.portfolio.pos_handler.positions[order.asset][
                    order.direction
                ].net_size
                if order.size > available_size:
                    LOG.info("Fixing order size")
                    order.size = available_size
            truncated_size = self.truncate_order_size(order)
            LOG.info("truncated order size: %s", truncated_size)
            exchange_to_lower = order.asset.exchange.lower()
            exchange_instance = self.ccxt_connector.get_exchange_instance(
                exchange_to_lower
            )
            order_response_dict = exchange_instance.createOrder(
                symbol=order.asset.symbol,
                type="market",
                side=order.action.name.lower(),
                amount=truncated_size,
            )
            parser = self.ccxt_connector.get_parser(exchange_to_lower)
            order_info = order_response_dict["info"]
            order_id, order_status = parser.parse_order_info(order_info)
            order.order_id = order_id
            LOG.info(
                "Market order successfuly submited with order id: %s", order.order_id
            )
            if order_status != "FILLED":
                self.open_orders_ids[exchange_to_lower].add(order.order_id)
            else:
                order.complete()
            self.currency_pairs_traded.add(order.asset)
        except Exception as e:
            error_message = get_rejected_order_error_message(order)
            LOG.error(error_message + "The exception is: %s", str(e))

    def execute_limit_order(self, limit_order: Order) -> None:
        try:
            LOG.info("Submit limit order to exchange")
            if limit_order.is_exit_order:
                self.update_balances_and_positions()
                available_size = self.portfolio.pos_handler.positions[
                    limit_order.asset
                ][limit_order.direction].net_size
                limit_order.size = min(limit_order.size, available_size)
            truncated_size = self.truncate_order_size(limit_order)
            exchange_to_lower = limit_order.asset.exchange.lower()
            exchange_instance = self.ccxt_connector.get_exchange_instance(
                exchange_to_lower
            )
            order_response_dict = exchange_instance.createOrder(
                symbol=limit_order.asset.symbol,
                type="limit",
                side=limit_order.action.name.lower(),
                amount=truncated_size,
                price=limit_order.limit,
            )
            parser = self.ccxt_connector.get_parser(exchange_to_lower)
            order_info = order_response_dict["info"]
            order_id, order_status = parser.parse_order_info(order_info)
            limit_order.order_id = order_id
            LOG.info(
                "Limit order successfuly submited with order id: %s",
                limit_order.order_id,
            )
            self.open_orders_ids[exchange_to_lower].add(limit_order.order_id)
            self.currency_pairs_traded.add(limit_order.asset)
        except Exception as e:
            error_message = get_rejected_order_error_message(limit_order)
            LOG.error(error_message + "The exception is: %s", str(e))

    def execute_stop_order(self, stop_order: Order) -> None:
        LOG.info("Checking stop order")
        price = self.current_price(stop_order.asset)
        if (
            stop_order.action == Action.BUY
            and price >= stop_order.stop
            or stop_order.action == Action.SELL
            and price <= stop_order.stop
        ):
            self.open_orders_ids[stop_order.asset.exchange].discard(stop_order.order_id)
            self.execute_market_order(stop_order)
        else:
            self.open_orders.append(stop_order)
            self.open_orders_ids[stop_order.asset.exchange].add(stop_order.order_id)

    def execute_target_order(self, target_order: Order) -> None:
        LOG.info("Checking target order")
        price = self.current_price(target_order.asset)
        if (
            target_order.action == Action.BUY
            and price <= target_order.target
            or target_order.action == Action.SELL
            and price >= target_order.target
        ):
            self.execute_market_order(target_order)
        else:
            self.open_orders.append(target_order)

    def get_trailing_stop(self, trailing_stop_order: Order) -> float:
        price = self.current_price(trailing_stop_order.asset)
        if trailing_stop_order.action == Action.BUY:
            stop = price + price * trailing_stop_order.stop_pct
        else:
            stop = price - price * trailing_stop_order.stop_pct
        return stop

    def execute_trailing_stop_order(
        self,
        trailing_stop_order: Order,
    ) -> bool:
        price = self.current_price(trailing_stop_order.asset)
        stop = self.get_trailing_stop(trailing_stop_order)
        if trailing_stop_order.action == Action.BUY:
            if trailing_stop_order.stop is None:
                trailing_stop_order.stop = stop
            else:
                trailing_stop_order.stop = min(trailing_stop_order.stop, stop)
        else:
            if trailing_stop_order.stop is None:
                trailing_stop_order.stop = stop
            else:
                trailing_stop_order.stop = max(trailing_stop_order.stop, stop)

        LOG.info("stop: %s", stop)
        LOG.info("last stop: %s", trailing_stop_order.stop)
        if (
            trailing_stop_order.action == Action.BUY
            and price >= trailing_stop_order.stop
            or trailing_stop_order.action == Action.SELL
            and price <= trailing_stop_order.stop
        ):
            self.execute_market_order(trailing_stop_order)
        else:
            self.open_orders.append(trailing_stop_order)

    def execute_order(self, order: Order) -> None:
        """
        Execute an order according to its type
        Parameters
        ----------
        order : `Order`
            The Order instance to execute.
        """
        if order.order_type == OrderType.LIMIT:
            self.execute_limit_order(order)
        elif order.order_type == OrderType.STOP:
            self.execute_stop_order(order)
        elif order.order_type == OrderType.TARGET:
            self.execute_target_order(order)
        elif order.order_type == OrderType.TRAILING_STOP:
            self.execute_trailing_stop_order(order)
        elif order.order_type == OrderType.MARKET:
            self.execute_market_order(order)

    def current_price(self, asset: Asset):
        return self.last_prices[asset]

    def synchronize(self) -> None:
        self.update_price()
        self.update_lot_size_info()
        self.update_transactions()
        self.update_balances_and_positions()
