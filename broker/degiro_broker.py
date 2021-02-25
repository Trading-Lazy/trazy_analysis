import os
from typing import List

import pandas as pd

import settings
from broker import degiroapi
from broker.broker import Broker
from broker.common import get_rejected_order_error_message
from common.clock import Clock
from common.helper import get_or_create_nested_dict
from logger import logger
from models.enums import Action, Direction, OrderCondition, OrderType
from models.order import Order
from position.position import Position
from position.transaction import Transaction

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class DegiroBroker(Broker):
    # Every period the positions of the broker get updated
    UPDATE_POSITIONS_PERIOD = pd.Timedelta(value=10, unit="seconds")
    UPDATE_CASH_BALANCES_PERIOD = pd.Timedelta(value=10, unit="seconds")
    UPDATE_TRANSACTIONS_PERIOD = pd.Timedelta(value=10, unit="seconds")
    PRODUCT_INFO_PERIOD = pd.Timedelta(value=10, unit="days")
    SYMBOL_INFO_PERIOD = pd.Timedelta(value=10, unit="days")
    TRANSACTION_LOOKBACK_PERIOD = pd.offsets.Day(1)

    def __init__(
        self,
        clock: Clock,
        base_currency: str = "EUR",
        supported_currencies: List[str] = ["EUR", "USD"],
    ):
        super().__init__(
            clock=clock,
            base_currency=base_currency,
            supported_currencies=supported_currencies,
        )
        self.degiro = degiroapi.DeGiro()
        self.degiro.login(settings.DEGIRO_BROKER_LOGIN, settings.DEGIRO_BROKER_PASSWORD)
        self.product_id_to_symbol = {}
        self.symbol_to_product_id = {}
        self.product_info_last_update = {}
        self.symbol_info_last_update = {}
        self.cash_balances_last_update = None
        self.update_cash_balances()
        self.open_positions_last_update = None
        self.update_open_positions()
        self.portfolio.subscribe_funds(self.cash_balances[base_currency])
        self.transactions_last_update = None
        self.executed_orders_ids = set()
        self.open_orders_ids = set()
        self.update_transactions()
        self.pending_orders = {}

    def update_product_info(self, product_id: str) -> None:
        now = self.clock.current_time()
        if (
            product_id in self.product_info_last_update
            and now - self.product_info_last_update[product_id]
            < DegiroBroker.PRODUCT_INFO_PERIOD
        ):
            return
        info = self.degiro.product_info(product_id)
        product_symbol = info["symbol"]
        self.product_id_to_symbol[product_id] = product_symbol
        self.symbol_to_product_id[product_symbol] = product_id
        self.product_info_last_update[product_id] = self.clock.current_time()

    def update_symbol_info(self, symbol: str) -> None:
        now = self.clock.current_time()
        if (
            symbol in self.symbol_info_last_update
            and now - self.symbol_info_last_update[symbol]
            < DegiroBroker.PRODUCT_INFO_PERIOD
        ):
            return
        infos = self.degiro.search_products(symbol, limit=5)
        for info in infos:
            if info["symbol"] == symbol:
                product_id = info["id"]
                self.symbol_to_product_id[symbol] = product_id
                self.product_id_to_symbol[product_id] = symbol
                return

    def update_cash_balances(self) -> float:
        now = self.clock.current_time()
        if (
            self.cash_balances_last_update is not None
            and now - self.cash_balances_last_update
            < DegiroBroker.UPDATE_CASH_BALANCES_PERIOD
        ):
            return

        currency_cash_pairs = self.degiro.getdata(degiroapi.Data.Type.CASHFUNDS)
        for currency_cash_pair_string in currency_cash_pairs:
            currency_cash_pair = currency_cash_pair_string.split(sep=" ")
            currency = currency_cash_pair[0]
            if currency not in self.supported_currencies:
                continue
            cash = float(currency_cash_pair[1])
            self.cash_balances[currency] = cash
        self.cash_balances_last_update = self.clock.current_time()

    def update_open_positions(self):
        now = self.clock.current_time()
        if (
            self.open_positions_last_update is not None
            and now - self.open_positions_last_update
            < DegiroBroker.UPDATE_POSITIONS_PERIOD
        ):
            return
        open_positions = self.degiro.getdata(
            degiroapi.Data.Type.PORTFOLIO, filter_zero=True
        )
        map_product_id_to_position = {}
        product_ids = []
        for open_position in open_positions:
            if open_position["positionType"] == "PRODUCT":
                product_id = open_position["id"]
                product_ids.append(product_id)
                map_product_id_to_position[product_id] = open_position

        portfolio = self.portfolio
        positions = portfolio.pos_handler.positions
        for product_id in product_ids:
            self.update_product_info(product_id)
            product_symbol = self.product_id_to_symbol[product_id]
            open_position = map_product_id_to_position[product_id]
            initial_price = float(str(open_position["price"]))
            size = open_position["size"]
            if size > 0:
                direction = Direction.LONG
                buy_size = size
                sell_size = 0
            else:
                direction = Direction.SHORT
                buy_size = 0
                sell_size = size
            get_or_create_nested_dict(positions, product_symbol, direction)
            positions[product_symbol][direction] = Position(
                product_symbol,
                initial_price,
                buy_size,
                sell_size,
                direction,
            )
        self.open_positions_last_update = self.clock.current_time()

    def has_opened_position(self, symbol: str, direction: Direction) -> bool:
        self.update_open_positions()
        portfolio = self.portfolio
        positions = portfolio.pos_handler.positions
        return symbol in positions and direction in positions[symbol]

    def subscribe_funds_to_account(self, amount: float) -> None:  # pragma: no cover
        # TODO automate bank transfers to degiro account
        pass

    def withdraw_funds_from_account(self, amount: float) -> None:  # pragma: no cover
        # TODO automate bank transfers to degiro account
        pass

    def update_transactions(self):
        now = self.clock.current_time()
        if (
            self.transactions_last_update is not None
            and now - self.transactions_last_update
            < DegiroBroker.UPDATE_TRANSACTIONS_PERIOD
        ):
            return
        # get confirmed orders that are opened
        last_orders = self.degiro.orders(
            now - DegiroBroker.TRANSACTION_LOOKBACK_PERIOD,
            now,
            False,
        )
        potentially_executed_orders = {}
        for last_order in last_orders:
            if (
                last_order["isActive"]
                or last_order["status"] != "CONFIRMED"
                or last_order["type"] != "CREATE"
                or "last" not in last_order
            ):
                continue  # pragma: no cover
            product_id = last_order["productId"]
            if product_id not in potentially_executed_orders:
                potentially_executed_orders[product_id] = [last_order]
            else:
                potentially_executed_orders[product_id].append(last_order)
        degiro_transactions = self.degiro.transactions(
            now - DegiroBroker.TRANSACTION_LOOKBACK_PERIOD, now
        )
        for degiro_transaction in degiro_transactions:
            product_id = degiro_transaction["productId"]
            self.update_product_info(product_id)
            symbol = self.product_id_to_symbol[product_id]
            transaction_id = degiro_transaction["id"]
            timestampStr = degiro_transaction["date"]
            timestamp = pd.Timestamp(timestampStr, tz="UTC").to_pydatetime()
            size = int(abs(degiro_transaction["quantity"]))
            actionStr = degiro_transaction["buysell"]
            action = Action.BUY if actionStr == "B" else Action.SELL
            total = float(str(degiro_transaction["total"]))
            if action == Action.BUY:
                direction = Direction.LONG if total < 0 else Direction.SHORT
            else:
                direction = Direction.LONG if total > 0 else Direction.SHORT
            price = float(str(degiro_transaction["price"]))
            error_message = (
                f"There is a Transaction that occured for symbol {symbol} without any order potentially "
                f"linked to it. It is either a system bug or a broker dysfunction. The transaction id is"
                f" {transaction_id} and it occured at {timestamp}."
            )
            if product_id not in potentially_executed_orders:
                LOG.error(error_message)
                continue
            order = None
            found = False
            for index, potentially_executed_order in enumerate(
                potentially_executed_orders[product_id]
            ):
                if potentially_executed_order["last"] == timestampStr:
                    order = potentially_executed_orders[product_id][index]
                    found = True
                    break
            if not found:
                LOG.error(error_message)
                continue
            order_id = order["orderId"]
            LOG.info("Checking order with order id: %s", order_id)
            if order_id in self.executed_orders_ids:
                continue
            LOG.info("Order with order id: %s has been successfully executed", order_id)
            self.executed_orders_ids.add(order_id)
            self.open_orders_ids.discard(order_id)
            total_plus_fee_in_base_currency = float(
                str(degiro_transaction["totalPlusFeeInBaseCurrency"])
            )
            commission = abs(total_plus_fee_in_base_currency - total)
            transaction = Transaction(
                symbol=symbol,
                size=size,
                action=action,
                direction=direction,
                price=price,
                order_id=order_id,
                commission=commission,
                timestamp=timestamp,
                transaction_id=transaction_id,
            )
            self.portfolio.transact_symbol(transaction)
        LOG.info("Transactions have been synchronized")
        self.transactions_last_update = self.clock.current_time()

    def get_cash_balance(self, currency: str = None) -> float:
        self.update_cash_balances()
        if currency is None:
            return self.cash_balances

        if currency not in self.cash_balances.keys():
            raise ValueError(
                "Currency of type '%s' is not found within the "
                "broker cash master accounts. Could not retrieve "
                "cash balance." % currency
            )
        return self.cash_balances[currency]

    def get_action_func(self, action: Action):
        if action == Action.BUY:
            action_func = self.degiro.buyorder
        else:
            action_func = self.degiro.sellorder
        return action_func

    def execute_market_order(self, order: Order) -> None:
        try:
            self.update_symbol_info(order.symbol)
            product_id = self.symbol_to_product_id[order.symbol]
            action_func = self.get_action_func(order.action)
            LOG.info("Submit market order to degiro")
            execution_time = 1 if order.condition == OrderCondition.EOD else 3
            order.order_id = action_func(
                degiroapi.Order.Type.MARKET, product_id, execution_time, order.size
            )
            LOG.info("Order successfuly submited with order id: %s", order.order_id)
            self.open_orders_ids.add(order.order_id)
        except Exception as e:
            error_message = get_rejected_order_error_message(order)
            LOG.error(error_message + "The exception is: %s", str(e))

    def execute_limit_order(self, limit_order: Order) -> None:
        try:
            self.update_symbol_info(limit_order.symbol)
            product_id = self.symbol_to_product_id[limit_order.symbol]
            action_func = self.get_action_func(limit_order.action)
            execution_time = 1 if limit_order.condition == OrderCondition.EOD else 3
            limit_order.order_id = action_func(
                degiroapi.Order.Type.LIMIT,
                product_id,
                execution_time,
                limit_order.size,
                limit=limit_order.limit,
            )
            self.open_orders_ids.add(limit_order.order_id)
        except Exception as e:
            error_message = get_rejected_order_error_message(limit_order)
            LOG.error(error_message + "The exception is: %s", str(e))

    def execute_stop_order(self, stop_order: Order) -> None:
        try:
            self.update_symbol_info(stop_order.symbol)
            product_id = self.symbol_to_product_id[stop_order.symbol]
            action_func = self.get_action_func(stop_order.action)
            execution_time = 1 if stop_order.condition == OrderCondition.EOD else 3
            stop_order.order_id = action_func(
                degiroapi.Order.Type.STOPLOSS,
                product_id,
                execution_time,
                stop_order.size,
                stop_loss=stop_order.stop,
            )
            self.open_orders_ids.add(stop_order.order_id)
        except Exception as e:
            error_message = get_rejected_order_error_message(stop_order)
            LOG.error(error_message + "The exception is: %s", str(e))

    def execute_target_order(self, target_order: Order) -> None:
        price = self.current_price(target_order.symbol)
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
        price = self.current_price(trailing_stop_order.symbol)
        if trailing_stop_order.action == Action.BUY:
            stop = price + price * trailing_stop_order.stop_pct
        else:
            stop = price - price * trailing_stop_order.stop_pct
        return stop

    def execute_trailing_stop_order(
        self,
        trailing_stop_order: Order,
    ) -> bool:
        stop = self.get_trailing_stop(trailing_stop_order)
        LOG.info("stop: %s", stop)
        LOG.info("last stop: %s", trailing_stop_order.stop)
        if trailing_stop_order.stop is None:
            trailing_stop_order.stop = stop
            self.execute_stop_order(trailing_stop_order)
            self.open_orders.append(trailing_stop_order)
            return

        if trailing_stop_order.action == Action.BUY:
            if (
                stop < trailing_stop_order.stop
                and trailing_stop_order.order_id in self.open_orders_ids
            ):
                trailing_stop_order.stop = stop
                self.degiro.delete_order(trailing_stop_order.order_id)
                self.open_orders_ids.discard(trailing_stop_order.order_id)
                self.execute_stop_order(trailing_stop_order)
        else:
            if (
                stop > trailing_stop_order.stop
                and trailing_stop_order.order_id in self.open_orders_ids
            ):
                trailing_stop_order.stop = stop
                self.degiro.delete_order(trailing_stop_order.order_id)
                self.open_orders_ids.discard(trailing_stop_order.order_id)
                self.execute_stop_order(trailing_stop_order)

    def execute_order(self, order: Order) -> None:
        """
        Execute an order according to its type
        Parameters
        ----------
        order : `Order`
            The Order instance to execute.
        """
        if order.type == OrderType.LIMIT:
            self.execute_limit_order(order)
        elif order.type == OrderType.STOP:
            self.execute_stop_order(order)
        elif order.type == OrderType.TARGET:
            self.execute_target_order(order)
        elif order.type == OrderType.TRAILING_STOP:
            self.execute_trailing_stop_order(order)
        elif order.type == OrderType.MARKET:
            self.execute_market_order(order)

    def synchronize(self) -> None:
        LOG.info("Synchronize logical broker")
        self.update_transactions()
        self.update_open_positions()
        self.update_cash_balances()
