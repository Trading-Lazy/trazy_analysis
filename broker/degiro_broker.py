import os
import traceback
from collections import deque
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import List

import pandas as pd

import trazy_analysis.settings
from trazy_analysis.broker import degiroapi
from trazy_analysis.broker.broker import Broker
from trazy_analysis.broker.common import get_rejected_order_error_message
from trazy_analysis.common.clock import Clock
from trazy_analysis.common.constants import DEGIRO_DATETIME_FORMAT
from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.common.utils import timestamp_to_utc
from trazy_analysis.logger import logger
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Action, Direction, OrderCondition, OrderType
from trazy_analysis.models.order import Order
from trazy_analysis.portfolio.portfolio_event import PortfolioEvent
from trazy_analysis.position.position import Position
from trazy_analysis.position.transaction import Transaction

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class DegiroBroker(Broker):
    # Every period the positions of the broker get updated
    UPDATE_POSITIONS_PERIOD = pd.Timedelta(value=10, unit="seconds")
    UPDATE_CASH_BALANCES_PERIOD = pd.Timedelta(value=10, unit="seconds")
    UPDATE_TRANSACTIONS_PERIOD = pd.Timedelta(value=10, unit="seconds")
    PRODUCT_INFO_PERIOD = pd.Timedelta(value=10, unit="days")
    SYMBOL_INFO_PERIOD = pd.Timedelta(value=10, unit="days")
    TRANSACTION_LOOKBACK_PERIOD = pd.offsets.Day(1)
    DEGIRO_PRICE_MULTIPLICATOR = Decimal("0.01")
    DEGIRO_PRICE_DECIMAL_PLACES = 2

    def __init__(
        self,
        clock: Clock,
        events: deque,
        base_currency: str = "EUR",
        supported_currencies: List[str] = ["EUR", "USD"],
    ):
        exchange = "DEGIRO"
        super().__init__(
            clock=clock,
            events=events,
            base_currency=base_currency,
            supported_currencies=supported_currencies,
            exchange=exchange,
        )
        self.degiro = degiroapi.DeGiro()
        self.degiro.login(
            trazy_analysis.settings.DEGIRO_BROKER_LOGIN,
            trazy_analysis.settings.DEGIRO_BROKER_PASSWORD,
        )
        self.product_id_to_asset = {}
        self.asset_to_product_id = {}
        self.product_info_last_update = {}
        self.asset_info_last_update = {}
        self.cash_balances_last_update = None
        self.update_cash_balances()
        self.open_positions_last_update = None
        self.update_open_positions()
        self.transactions_last_update = self.clock.current_time()
        self.executed_orders_ids = set()
        self.open_orders_ids = set()
        self.processed_transactions_ids = set()
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
        asset = Asset(symbol=product_symbol, exchange=self.exchange)
        self.product_id_to_asset[product_id] = asset
        self.asset_to_product_id[asset] = product_id
        self.product_info_last_update[product_id] = self.clock.current_time()

    def update_asset_info(self, asset: Asset) -> None:
        now = self.clock.current_time()
        if (
            asset in self.asset_info_last_update
            and now - self.asset_info_last_update[asset]
            < DegiroBroker.PRODUCT_INFO_PERIOD
        ):
            return
        infos = self.degiro.search_products(asset.symbol, limit=5)
        for info in infos:
            if info["symbol"] == asset.symbol:  # TODO check also exchange
                product_id = info["id"]
                self.asset_to_product_id[asset] = product_id
                self.product_id_to_asset[product_id] = asset
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
        self.portfolio.cash = self.cash_balances[self.base_currency]
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
            asset = self.product_id_to_asset[product_id]
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
            get_or_create_nested_dict(positions, asset, direction)
            positions[asset][direction] = Position(
                asset,
                initial_price,
                buy_size,
                sell_size,
                direction,
            )
        self.open_positions_last_update = self.clock.current_time()

    def has_opened_position(self, asset: Asset, direction: Direction) -> bool:
        self.update_open_positions()
        portfolio = self.portfolio
        positions = portfolio.pos_handler.positions
        return asset in positions and direction in positions[asset]

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
        transactions_last_update = self.clock.current_time()
        # get confirmed orders that are opened
        last_orders = self.degiro.orders(
            self.transactions_last_update,
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
            # check time
            last_iso = last_order["last"][:-3] + last_order["last"][-2:]
            last_datetime = datetime.strptime(last_iso, DEGIRO_DATETIME_FORMAT)
            if last_datetime < self.transactions_last_update:
                continue
            product_id = last_order["productId"]
            if product_id not in potentially_executed_orders:
                potentially_executed_orders[product_id] = [last_order]
            else:
                potentially_executed_orders[product_id].append(last_order)
        degiro_transactions = self.degiro.transactions(
            self.transactions_last_update, now
        )
        processed_transactions_ids = set()
        for degiro_transaction in degiro_transactions:
            timestampStr = (
                degiro_transaction["date"][:-3] + degiro_transaction["date"][-2:]
            )
            timestamp = timestamp_to_utc(
                datetime.strptime(timestampStr, DEGIRO_DATETIME_FORMAT)
            )
            if timestamp < self.transactions_last_update:
                continue
            product_id = degiro_transaction["productId"]
            self.update_product_info(product_id)
            asset = self.product_id_to_asset[product_id]
            transaction_id = degiro_transaction["id"]
            if transaction_id in self.processed_transactions_ids:
                continue
            processed_transactions_ids.add(transaction_id)
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
                f"There is a Transaction that occured for asset {asset.key()} without any order potentially "
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
                if (
                    potentially_executed_order["created"]
                    <= timestampStr
                    <= potentially_executed_order["last"]
                ):
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
                    type="asset_transaction",
                    description=description,
                    debit=transaction.cost_with_commission,
                    credit=0.0,
                    balance=self.portfolio.cash,
                )
            else:
                pe = PortfolioEvent(
                    timestamp=transaction.timestamp,
                    type="asset_transaction",
                    description=description,
                    debit=0.0,
                    credit=-1.0 * round(transaction.cost_with_commission, 2),
                    balance=round(self.portfolio.cash, 2),
                )
            self.portfolio.history.append(pe)
            self.processed_transactions_ids.add(transaction_id)

        LOG.info("Transactions have been synchronized")
        self.processed_transactions_ids = processed_transactions_ids
        self.transactions_last_update = transactions_last_update

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
            self.update_asset_info(order.asset)
            product_id = self.asset_to_product_id[order.asset]
            action_func = self.get_action_func(order.action)
            LOG.info("Submit market order to degiro")
            execution_time = 1 if order.condition == OrderCondition.EOD else 3
            order.order_id = action_func(
                degiroapi.Order.Type.MARKET, product_id, execution_time, order.size
            )
            order.complete()
            LOG.info("Order successfuly submited with order id: %s", order.order_id)
            self.open_orders_ids.add(order.order_id)
        except Exception as e:
            error_message = get_rejected_order_error_message(order)
            LOG.error(error_message + "The exception is: %s", str(e))

    def truncate_price(self, price: float, roungind=ROUND_DOWN) -> float:
        decimal_price = Decimal(str(price))
        decimal_truncated_price = decimal_price.quantize(
            DegiroBroker.DEGIRO_PRICE_MULTIPLICATOR, rounding=roungind
        )
        truncated_price = round(
            float(decimal_truncated_price), DegiroBroker.DEGIRO_PRICE_DECIMAL_PLACES
        )
        return truncated_price

    def execute_limit_order(self, limit_order: Order) -> None:
        try:
            self.update_asset_info(limit_order.asset)
            product_id = self.asset_to_product_id[limit_order.asset]
            action_func = self.get_action_func(limit_order.action)
            execution_time = 1 if limit_order.condition == OrderCondition.EOD else 3
            rounding = ROUND_DOWN if limit_order.action == Action.BUY else ROUND_UP
            truncated_limit = self.truncate_price(limit_order.limit, rounding)
            LOG.info("Submit limit order to degiro")
            limit_order.order_id = action_func(
                degiroapi.Order.Type.LIMIT,
                product_id,
                execution_time,
                limit_order.size,
                limit=truncated_limit,
            )
            LOG.info(
                "Order successfuly submited with order id: %s", limit_order.order_id
            )
            self.open_orders_ids.add(limit_order.order_id)
        except Exception as e:
            error_message = get_rejected_order_error_message(limit_order)
            LOG.error(
                error_message + "The exception is: %s. The traceback is %s",
                str(e),
                traceback.format_exc(),
            )

    def execute_stop_order(self, stop_order: Order) -> None:
        try:
            self.update_asset_info(stop_order.asset)
            product_id = self.asset_to_product_id[stop_order.asset]
            action_func = self.get_action_func(stop_order.action)
            execution_time = 1 if stop_order.condition == OrderCondition.EOD else 3
            rounding = ROUND_DOWN if stop_order.action == Action.BUY else ROUND_UP
            truncated_stop = self.truncate_price(stop_order.stop, rounding)
            stop_order.stop = truncated_stop
            LOG.info("Submit stop order to degiro")
            stop_order.order_id = action_func(
                degiroapi.Order.Type.STOPLOSS,
                product_id,
                execution_time,
                stop_order.size,
                stop_loss=truncated_stop,
            )
            LOG.info(
                "Order successfuly submited with order id: %s", stop_order.order_id
            )
            self.open_orders_ids.add(stop_order.order_id)
        except Exception as e:
            error_message = get_rejected_order_error_message(stop_order)
            LOG.error(
                error_message + "The exception is: %s. The traceback is %s",
                str(e),
                traceback.format_exc(),
            )

    def execute_target_order(self, target_order: Order) -> None:
        price = self.current_price(target_order.asset)
        LOG.info("target: %s", target_order.target)
        LOG.info("Checking if target has been reached")
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

    # def execute_trailing_stop_order(
    #     self,
    #     trailing_stop_order: Order,
    # ) -> bool:
    #     stop = self.get_trailing_stop(trailing_stop_order)
    #     LOG.info("stop: %s", stop)
    #     LOG.info("last stop: %s", trailing_stop_order.stop)
    #     LOG.info("Checking if trailing stop has been reached")
    #     if trailing_stop_order.stop is None:
    #         trailing_stop_order.stop = stop
    #         self.execute_stop_order(trailing_stop_order)
    #         self.open_orders.append(trailing_stop_order)
    #         return
    #
    #     if trailing_stop_order.action == Action.BUY:
    #         if (
    #             stop < trailing_stop_order.stop
    #             and trailing_stop_order.order_id in self.open_orders_ids
    #         ):
    #             trailing_stop_order.stop = stop
    #             self.degiro.delete_order(trailing_stop_order.order_id)
    #             self.open_orders_ids.discard(trailing_stop_order.order_id)
    #             self.execute_stop_order(trailing_stop_order)
    #     else:
    #         if (
    #             stop > trailing_stop_order.stop
    #             and trailing_stop_order.order_id in self.open_orders_ids
    #         ):
    #             trailing_stop_order.stop = stop
    #             self.degiro.delete_order(trailing_stop_order.order_id)
    #             self.open_orders_ids.discard(trailing_stop_order.order_id)
    #             self.execute_stop_order(trailing_stop_order)

    def execute_trailing_stop_order(
        self,
        trailing_stop_order: Order,
    ) -> bool:
        LOG.info("Checking if trailing stop loss")
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
        self.update_cash_balances()
        self.update_transactions()
        self.update_open_positions()
