import logging
import os
import time
from decimal import Decimal
from typing import List

import settings
from db_storage.db_storage import DbStorage
from logger import logger
from models.order import Order
from models.candle import Candle
from models.enums import Action, Direction
from strategy.strategy import Strategy


class Simulation:
    def reset(self) -> None:
        epoch_time_now = int(time.time())
        simulation_name = "{}_{}_{}".format(
            self.strategy.name, self.symbol, epoch_time_now
        )
        self.logger = (
            logger.get_root_logger(
                simulation_name,
                filename=os.path.join(
                    settings.ROOT_PATH, "{}.log".format(simulation_name)
                ),
            )
            if self.log
            else logging
        )
        self.candles = None
        self.cash = Decimal("0")
        self.commission = Decimal("0")
        self.portfolio_value = Decimal("0")
        self.last_transaction_price = {
            Direction.LONG: {
                Action.BUY: Decimal("0"),
                Action.SELL: Decimal("0"),
            },
            Direction.SHORT: {
                Action.SELL: Decimal("0"),
                Action.BUY: Decimal("0"),
            },
        }
        self.shares_amounts = {
            Direction.LONG: 0,
            Direction.SHORT: 0,
        }

    def __init__(
        self,
        strategy: Strategy,
        db_storage: DbStorage,
        candles: List[Candle] = None,
        cash: Decimal = Decimal("0"),
        commission: Decimal = Decimal("0"),
        symbol: str = "symbol",
        log: bool = True,
    ):
        epoch_time_now = int(time.time())
        self.symbol = symbol
        self.strategy = strategy
        self.db_storage = db_storage
        simulation_name = "{}_{}_{}".format(
            self.strategy.name, self.symbol, epoch_time_now
        )
        self.log = log
        self.logger = (
            logger.get_root_logger(
                simulation_name,
                filename=os.path.join(
                    settings.ROOT_PATH, "{}.log".format(simulation_name)
                ),
            )
            if self.log
            else logging
        )
        self.candles = candles
        self.cash = cash
        self.commission = commission
        self.portfolio_value = Decimal("0")
        self.last_transaction_price = {
            Direction.LONG: {
                Action.BUY: Decimal("0"),
                Action.SELL: Decimal("0"),
            },
            Direction.SHORT: {
                Action.SELL: Decimal("0"),
                Action.BUY: Decimal("0"),
            },
        }
        self.shares_amounts = {
            Direction.LONG: 0,
            Direction.SHORT: 0,
        }

    def fund(self, cash: Decimal) -> None:
        self.cash += cash

    def get_orders(self) -> List[Order]:
        orders = []
        for candle in self.candles:
            order = self.strategy.generate_signal(candle)
            if order is None:
                continue
            orders.append(order)
        return orders

    def is_closed_position(self, direction: Direction, action: Action):
        return (
                direction == Direction.LONG
                and action == Action.SELL
                or direction == Direction.SHORT
                and action == Action.BUY
        )

    def validate_shares_amounts(
        self, direction: Direction, action: Action, amount: int
    ):
        exception_str = "For a {} position, you cannot {} more than you {}".format(
            direction.name,
            action.name,
            action.BUY.name
            if action == action.SELL.name
            else action.SELL.name,
        )
        if (
            self.is_closed_position(direction, action)
            and abs(self.shares_amounts[direction]) < amount
        ):
            raise Exception(exception_str)

    def validate_cash(self, action: Action, total_cost_estimate):
        if action == Action.BUY and self.cash < total_cost_estimate:
            raise Exception(
                "Cannot update cash, cash has to be greater than or equal to the cost estimate. Cash: {}, "
                "Cost estimate "
                "with commissions added: {}".format(self.cash, total_cost_estimate)
            )

    def validate_transaction(
        self,
        direction: Direction,
        action: Action,
        amount: int,
        total_cost_estimate: int,
    ):
        self.validate_shares_amounts(direction, action, amount)
        self.validate_cash(action, total_cost_estimate)

    def update_shares_amounts(
        self, direction: Direction, action: Action, amount: int
    ):
        if action == Action.BUY:
            self.shares_amounts[direction] += amount
        else:
            self.shares_amounts[direction] -= amount

    def update_portfolio_value(
        self, direction: Direction, unit_cost_estimate: Decimal
    ):
        self.portfolio_value = self.shares_amounts[direction] * unit_cost_estimate

    def update_cash(
        self,
        action: Action,
        cost_estimate: Decimal,
        commission_amount: Decimal,
    ):
        if action == Action.BUY:
            self.cash -= cost_estimate
        else:
            self.cash += cost_estimate
        self.cash -= commission_amount

    def compute_profit(self, direction: Direction) -> Decimal:
        profit = (
            self.last_transaction_price[direction][Action.SELL]
            - self.last_transaction_price[direction][Action.BUY]
        )
        return profit

    def log_state(self):
        self.logger.info("Cash: {}".format(self.cash))
        self.logger.info("Portfolio value: {}".format(self.portfolio_value))
        self.logger.info("Total value: {}".format(self.cash + self.portfolio_value))

    def position(
        self,
        direction: Direction,
        action: Action,
        unit_cost_estimate: Decimal,
        amount: int,
    ):
        cost_estimate = unit_cost_estimate * amount
        commission_amount = cost_estimate * self.commission
        total_cost_estimate = cost_estimate + commission_amount

        self.last_transaction_price[direction][action] = total_cost_estimate

        self.validate_transaction(
            direction, action, amount, total_cost_estimate
        )

        self.update_shares_amounts(direction, action, amount)

        self.update_portfolio_value(direction, unit_cost_estimate)

        self.update_cash(action, cost_estimate, commission_amount)

        action_str = "{} {} {} shares at unit cost {}, total: {}, commission: {}".format(
            action.name,
            direction.name,
            amount,
            unit_cost_estimate,
            cost_estimate,
            cost_estimate * self.commission,
        )
        self.logger.info(action_str)

        if self.is_closed_position(direction, action):
            profit = self.compute_profit(Direction.LONG)
            self.logger.info("TRANSACTION PROFIT {}".format(profit))

        # Log state after transaction
        self.log_state()

    def run(self):
        self.logger.info("Starting funds: {}".format(self.cash))
        orders = self.get_orders()
        for order in orders:
            candle = self.db_storage.get_candle_by_identifier(
                order.symbol, order.root_candle_timestamp
            )
            unit_cost_estimate = Decimal(candle.close)
            self.position(
                order.direction,
                order.action,
                unit_cost_estimate,
                order.size,
            )

        self.logger.info("Final state:")
        self.log_state()
