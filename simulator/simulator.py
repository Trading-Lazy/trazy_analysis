from datetime import datetime
from typing import List

import pandas as pd
from pandas import DataFrame

import settings
import time
import multiprocessing
from logger import logger, os
from strategy.strategy import Strategy
from actionsapi.models import PositionType, ActionType, Action, Candle
from common.utils import validate_dataframe_columns


class Simulation:

    def reset(self):
        self.portfolio_value = 0
        self.last_transaction_price = {
            PositionType.LONG: {
                ActionType.BUY: 0,
                ActionType.SELL: 0
            },
            PositionType.SHORT: {
                ActionType.SELL: 0,
                ActionType.BUY: 0
            }
        }
        self.shares_amounts = {
            PositionType.LONG: 0,
            PositionType.SHORT: 0
        }

    def __init__(self, strategy: Strategy, candles: List[Candle] = None, cash: float = 0, commission: float = 0,
                 symbol: str = "symbol"):
        epoch_time_now = int(time.time())
        self.symbol = symbol
        self.strategy = strategy
        simulation_name = '{}_{}_{}'.format(self.strategy.name, self.symbol, epoch_time_now)
        self.logger = logger.get_root_logger(simulation_name, filename=os.path.join(settings.ROOT_PATH,
                                                                                    '{}.log'.format(simulation_name)))
        self.candles = candles
        self.cash = cash
        self.commission = commission
        self.portfolio_value = None
        self.last_transaction_price = None
        self.shares_amounts = None
        self.reset()

    def fund(self, cash: float):
        self.cash += cash

    def get_actions(self) -> List[Action]:
        actions = []
        for candle in self.candles:
            action = self.strategy.compute_action(candle)
            if action is None:
                continue
            actions.append(action)
        return actions

    def validate_transaction(self, position_type: PositionType, action_type: ActionType, amount: int):
        to_be_checked = {
            ActionType.BUY: {
                PositionType.SHORT: True,
                PositionType.LONG: False
            },
            ActionType.SELL: {
                PositionType.SHORT: False,
                PositionType.LONG: True
            }
        }
        exception_str = "For a {} position, you cannot {} more than you {}". \
            format(position_type.name,
                   action_type.name,
                   action_type.BUY.name if action_type == action_type.SELL.name else action_type.SELL.name)
        if to_be_checked[action_type][position_type] and abs(self.shares_amounts[position_type]) < amount:
            raise Exception(exception_str)

    def update_shares_amounts(self, position_type: PositionType, action_type: ActionType, amount: int):
        self.validate_transaction(position_type, action_type, amount)
        if action_type == ActionType.BUY:
            self.shares_amounts[position_type] += amount
        else:
            self.shares_amounts[position_type] -= amount

    def update_portfolio_value(self, position_type: PositionType, unit_cost_estimate: float):
        self.portfolio_value = self.shares_amounts[
                                   position_type] * unit_cost_estimate

    def update_cash(self, action_type: ActionType, cost_estimate: float):
        if action_type == ActionType.BUY:
            self.cash -= cost_estimate
        else:
            self.cash += cost_estimate

    def position(self, position_type: PositionType, action_type: ActionType, unit_cost_estimate: float, amount: int):
        cost_estimate = unit_cost_estimate * amount
        action_str = "{} {} {} shares at unit cost {}, total: {}". \
            format(action_type.name, position_type.name, amount, unit_cost_estimate, cost_estimate)
        if action_type == ActionType.BUY and self.cash < cost_estimate:
            self.logger.info("Not enough fund to take position: {}. Current funds: {}".format(action_str, self.cash))
            return

        self.logger.info(action_str)

        self.last_transaction_price[position_type][action_type] = cost_estimate

        self.update_shares_amounts(position_type, action_type, amount)

        self.update_portfolio_value(position_type, unit_cost_estimate)

        self.update_cash(action_type, cost_estimate)

        # Log state after transaction
        self.logger.info('Cash: {}'.format(self.cash))
        self.logger.info('Portfolio value: {}'.format(self.portfolio_value))

    def log_profit(self, position_type: PositionType):
        profit = self.last_transaction_price[position_type][ActionType.SELL] - \
                 self.last_transaction_price[position_type][ActionType.BUY]
        self.logger.info("TRANSACTION PROFIT {}".format(profit))

    def run(self):
        self.logger.info("Starting funds: {}".format(self.cash))
        actions = self.get_actions()
        for action in actions:
            candle = Candle.objects.get(id=int(action.candle_id))
            unit_cost_estimate = float(candle.close) * (1 + self.commission)
            self.position(action.position_type, action.action_type, unit_cost_estimate, action.amount)

        self.logger.info("Final state:")
        self.logger.info('Cash: {}'.format(self.cash))
        self.logger.info('Portfolio value: {}'.format(self.portfolio_value))


class Simulator:
    def __init__(self, symbol: str = "symbol"):
        self.cash = 0
        self.commission = 0
        self.candles = []
        self.simulations = []
        self.symbol = symbol

    def add_strategy(self, strategy: Strategy):
        simulation = Simulation(strategy, self.candles, self.cash, self.commission, self.symbol)
        self.simulations.append(simulation)

    def add_candles_from_dataframe(self, df: DataFrame):
        required_columns = ['id', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'timestamp']
        validate_dataframe_columns(df, required_columns)
        self.candles = []
        self.symbol = df[['symbol']].iloc[0]
        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].astype(float)
        df[['id', 'volume']] = df[['id', 'volume']].astype(int)

        for index, row in df.iterrows():
            candle = Candle(
                id=row['id'],
                symbol=row['symbol'],
                open=row['open'],
                high=row['high'],
                low=row['low'],
                close=row['close'],
                volume=row['volume'],
                timestamp=row['timestamp'].to_pydatetime()
            )
            self.candles.append(candle)

    def add_candles_data_from_db(self, symbol: str, start: datetime,
                                 end: datetime = datetime.now()):
        self.candles = []
        self.symbol = symbol
        self.candles = Candle.objects.all().filter(
            symbol=symbol,
            timestamp__gte=start,
            timestamp__lt=end
        )

    def add_candles_csv_data(self, csv_file_path):
        self.candles = []
        df = pd.read_csv(csv_file_path)
        self.add_candles_from_dataframe(df)

    def set_candles(self, candles: List[Candle]):
        self.candles = candles
        for simulation in self.simulations:
            simulation.candles = candles

    def fund(self, cash: float):
        self.cash += cash
        for simulation in self.simulations:
            simulation.fund(self.cash)

    def set_commission(self, commission: float):
        self.commission = commission
        for simulation in self.simulations:
            simulation.commission = self.commission

    def run(self):
        for simulation in self.simulations:
            p = multiprocessing.Process(target=simulation.run)
            p.start()


if __name__ == "__main__":
    pass
