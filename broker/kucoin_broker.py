import os
import traceback
from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List

import pandas as pd
from kucoin.client import Market, Trade, User

import trazy_analysis.settings
from trazy_analysis.broker.broker import Broker
from trazy_analysis.broker.ccxt_parser import KucoinParser
from trazy_analysis.broker.common import get_rejected_order_error_message
from trazy_analysis.broker.kucoin_fee_model import KucoinFeeModel
from trazy_analysis.common.clock import Clock
from trazy_analysis.common.constants import CONNECTION_ERROR_MESSAGE
from trazy_analysis.common.helper import datetime_to_epoch, get_or_create_nested_dict
from trazy_analysis.logger import logger
from trazy_analysis.market_data.common import datetime_from_epoch
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction, OrderType
from trazy_analysis.models.order import Order
from trazy_analysis.portfolio.portfolio_event import PortfolioEvent
from trazy_analysis.position.position import Position
from trazy_analysis.position.transaction import Transaction
from trazy_analysis.settings import KUCOIN_API_KEY, KUCOIN_API_SECRET

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class KucoinBroker(Broker):
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
        base_currency: str = "USDT",
        supported_currencies: List[str] = ["EUR", "USDT"],
    ):
        fee_model = KucoinFeeModel()
        parser = KucoinParser
        exchange = "KUCOIN"
        super().__init__(
            clock=clock,
            events=events,
            base_currency=base_currency,
            supported_currencies=supported_currencies,
            fee_model=fee_model,
            parser=parser,
            execute_at_end_of_day=False,
            exchange=exchange,
        )
        self.market_client = Market(url="https://api.kucoin.com")
        self.trade_client = Trade(
            key=KUCOIN_API_KEY,
            secret=KUCOIN_API_SECRET,
        )
        self.user_client = User(
            key=KUCOIN_API_KEY,
            secret=KUCOIN_API_SECRET,
        )
        self.price_last_update = None
        self.currency_pairs_traded = set()
        self.lot_size = {}
        self.lot_size_last_update = None
        self.update_lot_size_info()
        self.update_price()
        self.balances_last_update = None
        self.update_balances_and_positions()
        self.transactions_last_update: datetime = self.clock.current_time()
        self.open_orders_ids = set()
        self.update_transactions()

    def update_lot_size_info(self) -> None:
        now = self.clock.current_time()
        if (
            self.lot_size_last_update is not None
            and now - self.lot_size_last_update < KucoinBroker.UPDATE_LOT_SIZE_INFO
        ):
            return
        try:
            symbols_info = self.market_client.get_symbol_list()
        except Exception as e:
            LOG.warning(
                CONNECTION_ERROR_MESSAGE,
                str(e),
                traceback.format_exc(),
            )
            return

        for symbol_info in symbols_info:
            symbol, symbol_lot_size = self.parser.parse_lot_size_info(symbol_info)
            asset = Asset(symbol=symbol, exchange=self.exchange)
            self.lot_size[asset] = symbol_lot_size
        self.lot_size_last_update = self.clock.current_time()

    def update_price(self, candle: Candle = None) -> None:
        now = self.clock.current_time()
        if (
            self.price_last_update is not None
            and now - self.price_last_update < KucoinBroker.UPDATE_PRICE_PERIOD
        ):
            return
        try:
            prices_response = self.market_client.get_all_tickers()
            prices_dict = prices_response["ticker"]
        except Exception as e:
            LOG.warning(
                CONNECTION_ERROR_MESSAGE,
                str(e),
                traceback.format_exc(),
            )
            return
        for price_dict in prices_dict:
            symbol, price = self.parser.parse_price_info(price_dict)
            asset = Asset(symbol=symbol, exchange=self.exchange)
            self.last_prices[asset] = price
            self.portfolio.update_market_value_of_symbol(asset, price, now)
        self.price_last_update = self.clock.current_time()

    def update_balances(self) -> Dict[str, float]:
        try:
            accounts = self.user_client.get_account_list()
        except Exception as e:
            LOG.warning(
                CONNECTION_ERROR_MESSAGE,
                str(e),
                traceback.format_exc(),
            )
            return {}
        balances = self.parser.parse_balances_info(accounts)

        # cash balances
        for currency in self.supported_currencies:
            if currency not in balances:
                continue
            self.cash_balances[currency] = balances[currency]
            del balances[currency]

        self.portfolio.cash = self.cash_balances[self.base_currency]
        return balances

    def update_positions(self, open_balances: Dict[str, float]) -> None:
        # open positions
        portfolio = self.portfolio
        positions = portfolio.pos_handler.positions
        for symbol in open_balances:
            size = open_balances[symbol]
            if size > 0:
                direction = Direction.LONG
                buy_size = size
                sell_size = 0
            else:
                direction = Direction.SHORT
                buy_size = 0
                sell_size = size

            currency_pair = Asset(
                symbol=symbol + "/" + self.base_currency, exchange=self.exchange
            )
            currency_pair_reversed = Asset(
                symbol=self.base_currency + "/" + symbol, exchange=self.exchange
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
            and now - self.balances_last_update < KucoinBroker.UPDATE_BALANCES_PERIOD
        ):
            return

        open_balances = self.update_balances()

        self.update_positions(open_balances)

        self.balances_last_update = self.clock.current_time()

        LOG.info("Balances and positions have been updated")

    def has_opened_position(self, asset: Asset, direction: Direction) -> bool:
        asset_mapped = asset
        self.update_balances_and_positions()
        portfolio = self.portfolio
        positions = portfolio.pos_handler.positions
        if asset_mapped not in positions or direction not in positions[asset_mapped]:
            return False
        position = positions[asset_mapped][direction]
        return position.net_size >= self.lot_size[asset_mapped]

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
            < KucoinBroker.UPDATE_TRANSACTIONS_PERIOD
        ):
            return

        # get recent orders filled
        epoch_ms = datetime_to_epoch(self.transactions_last_update, 1000)
        try:
            trades_dict = self.trade_client.get_fill_list(
                tradeType="TRADE", startAt=epoch_ms
            )
        except Exception as e:
            LOG.warning(
                CONNECTION_ERROR_MESSAGE,
                str(e),
                traceback.format_exc(),
            )
            return
        trades = trades_dict["items"]
        for trade in trades:
            (
                trade_epoch_ms,
                symbol,
                size,
                action,
                price,
                order_id,
                commission,
                transaction_id,
            ) = self.parser.parse_trade_info(trade)
            if trade_epoch_ms < epoch_ms:
                continue
            timestamp = datetime_from_epoch(trade_epoch_ms)
            asset = Asset(symbol=symbol, exchange=self.exchange)
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

        if currency not in self.cash_balances.keys():
            raise ValueError(
                "Currency of type '%s' is not found within the "
                "broker cash master accounts. Could not retrieve "
                "cash balance." % currency
            )
        return self.cash_balances[currency]

    def max_entry_order_size(
        self, asset: Asset, direction: Direction, cash: float = None
    ) -> int:
        if cash is None:
            cash = self.portfolio.cash
        price = self.current_price(asset)
        return self.fee_model.calc_max_size_for_cash(cash=cash, price=price)

    def position_size(self, symbol: str, direction: Direction) -> int:
        return super().position_size(symbol, direction)

    def symbol_mapping(self, symbol: str) -> str:
        for currency in KucoinBroker.CURRENCY_MAPPING:
            mapping = KucoinBroker.CURRENCY_MAPPING[currency]
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
            LOG.info("Submit market order to kucoin")
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
            symbol = order.asset.symbol.replace("/", "-")
            side = order.action.BUY.name.lower()
            order_response = self.trade_client.create_market_order(
                symbol=symbol, side=side, size=float(truncated_size)
            )
            order_id, order_status = self.parser.parse_order_info(order_response)
            order.order_id = order_id
            self.open_orders_ids.add(order.order_id)
            self.currency_pairs_traded.add(order.asset)
            order.complete()
            LOG.info(
                "Market order successfuly submited with order id: %s", order.order_id
            )
        except Exception as e:
            error_message = get_rejected_order_error_message(order)
            LOG.error(error_message + "The exception is: %s", str(e))

    def execute_limit_order(self, limit_order: Order) -> None:
        try:
            LOG.info("Submit limit order to kucoin")
            if limit_order.is_exit_order:
                self.update_balances_and_positions()
                available_size = self.portfolio.pos_handler.positions[
                    limit_order.asset
                ][limit_order.direction].net_size
                limit_order.size = min(limit_order.size, available_size)
            truncated_size = self.truncate_order_size(limit_order)
            symbol = limit_order.asset.symbol.replace("/", "-")
            side = limit_order.action.BUY.name.lower()
            order_response = self.trade_client.create_limit_order(
                symbol=symbol,
                side=side,
                size=float(truncated_size),
                price=limit_order.limit,
            )
            limit_order.order_id = str(order_response["orderId"])
            self.open_orders_ids.add(limit_order.order_id)
            self.currency_pairs_traded.add(limit_order.asset)
            LOG.info(
                "Limit order successfuly submited with order id: %s",
                limit_order.order_id,
            )
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
            self.open_orders_ids.discard(stop_order.order_id)
            self.execute_market_order(stop_order)
        else:
            self.open_orders.append(stop_order)
            self.open_orders_ids.add(stop_order.order_id)

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

    def synchronize(self) -> None:
        self.update_price()
        self.update_lot_size_info()
        self.update_transactions()
        self.update_balances_and_positions()
