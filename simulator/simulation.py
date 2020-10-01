import logging
import os
import time
from decimal import Decimal
from typing import List

import settings
from db_storage.db_storage import DbStorage
from logger import logger
from models.action import Action
from models.candle import Candle
from models.enums import ActionType, PositionType
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
            PositionType.LONG: {
                ActionType.BUY: Decimal("0"),
                ActionType.SELL: Decimal("0"),
            },
            PositionType.SHORT: {
                ActionType.SELL: Decimal("0"),
                ActionType.BUY: Decimal("0"),
            },
        }
        self.shares_amounts = {
            PositionType.LONG: 0,
            PositionType.SHORT: 0,
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
            PositionType.LONG: {
                ActionType.BUY: Decimal("0"),
                ActionType.SELL: Decimal("0"),
            },
            PositionType.SHORT: {
                ActionType.SELL: Decimal("0"),
                ActionType.BUY: Decimal("0"),
            },
        }
        self.shares_amounts = {
            PositionType.LONG: 0,
            PositionType.SHORT: 0,
        }

    def fund(self, cash: Decimal) -> None:
        self.cash += cash

    def get_actions(self) -> List[Action]:
        actions = []
        for candle in self.candles:
            action = self.strategy.compute_action(candle)
            if action is None:
                continue
            actions.append(action)
        return actions

    def is_closed_position(self, position_type: PositionType, action_type: ActionType):
        return (
            position_type == PositionType.LONG
            and action_type == ActionType.SELL
            or position_type == PositionType.SHORT
            and action_type == ActionType.BUY
        )

    def validate_shares_amounts(
        self, position_type: PositionType, action_type: ActionType, amount: int
    ):
        exception_str = "For a {} position, you cannot {} more than you {}".format(
            position_type.name,
            action_type.name,
            action_type.BUY.name
            if action_type == action_type.SELL.name
            else action_type.SELL.name,
        )
        if (
            self.is_closed_position(position_type, action_type)
            and abs(self.shares_amounts[position_type]) < amount
        ):
            raise Exception(exception_str)

    def validate_cash(self, action_type: ActionType, total_cost_estimate):
        if action_type == ActionType.BUY and self.cash < total_cost_estimate:
            raise Exception(
                "Cannot update cash, cash has to be greater than or equal to the cost estimate. Cash: {}, "
                "Cost estimate "
                "with commissions added: {}".format(self.cash, total_cost_estimate)
            )

    def validate_transaction(
        self,
        position_type: PositionType,
        action_type: ActionType,
        amount: int,
        total_cost_estimate: int,
    ):
        self.validate_shares_amounts(position_type, action_type, amount)
        self.validate_cash(action_type, total_cost_estimate)

    def update_shares_amounts(
        self, position_type: PositionType, action_type: ActionType, amount: int
    ):
        if action_type == ActionType.BUY:
            self.shares_amounts[position_type] += amount
        else:
            self.shares_amounts[position_type] -= amount

    def update_portfolio_value(
        self, position_type: PositionType, unit_cost_estimate: Decimal
    ):
        self.portfolio_value = self.shares_amounts[position_type] * unit_cost_estimate

    def update_cash(
        self,
        action_type: ActionType,
        cost_estimate: Decimal,
        commission_amount: Decimal,
    ):
        if action_type == ActionType.BUY:
            self.cash -= cost_estimate
        else:
            self.cash += cost_estimate
        self.cash -= commission_amount

    def compute_profit(self, position_type: PositionType) -> Decimal:
        profit = (
            self.last_transaction_price[position_type][ActionType.SELL]
            - self.last_transaction_price[position_type][ActionType.BUY]
        )
        return profit

    def log_state(self):
        self.logger.info("Cash: {}".format(self.cash))
        self.logger.info("Portfolio value: {}".format(self.portfolio_value))
        self.logger.info("Total value: {}".format(self.cash + self.portfolio_value))

    def position(
        self,
        position_type: PositionType,
        action_type: ActionType,
        unit_cost_estimate: Decimal,
        amount: int,
    ):
        cost_estimate = unit_cost_estimate * amount
        commission_amount = cost_estimate * self.commission
        total_cost_estimate = cost_estimate + commission_amount

        self.last_transaction_price[position_type][action_type] = total_cost_estimate

        self.validate_transaction(
            position_type, action_type, amount, total_cost_estimate
        )

        self.update_shares_amounts(position_type, action_type, amount)

        self.update_portfolio_value(position_type, unit_cost_estimate)

        self.update_cash(action_type, cost_estimate, commission_amount)

        action_str = "{} {} {} shares at unit cost {}, total: {}, commission: {}".format(
            action_type.name,
            position_type.name,
            amount,
            unit_cost_estimate,
            cost_estimate,
            cost_estimate * self.commission,
        )
        self.logger.info(action_str)

        if self.is_closed_position(position_type, action_type):
            profit = self.compute_profit(PositionType.LONG)
            self.logger.info("TRANSACTION PROFIT {}".format(profit))

        # Log state after transaction
        self.log_state()

    def run(self):
        self.logger.info("Starting funds: {}".format(self.cash))
        actions = self.get_actions()
        for action in actions:
            candle = self.db_storage.get_candle_by_identifier(
                action.symbol, action.candle_timestamp
            )
            unit_cost_estimate = Decimal(candle.close)
            self.position(
                action.position_type,
                action.action_type,
                unit_cost_estimate,
                action.size,
            )

        self.logger.info("Final state:")
        self.log_state()
