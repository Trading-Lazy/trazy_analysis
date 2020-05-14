import datetime
from enum import auto, Enum

import pandas as pd
from pandas import DataFrame

import settings
from logger import logger, os
from strategy.action import PositionType, ActionType
from strategy.candlefetcher import CandleFetcher
from strategy.constants import DATE_FORMAT
from strategy.strategy import Strategy, Candle
from common.utils import validate_dataframe_columns

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, 'simulation.log'))


class PositionType(Enum):
    LONG = auto()
    SHORT = auto()


class Simulator:
    def __init__(self, name):
        epoch_time_now = datetime.now().timetuple()
        self.name = name
        self.logger = logger.get_root_logger(__name__, filename=os.path.join(settings.ROOT_PATH,
                                                                             '{}_{}.log'.format(self.name,
                                                                                                epoch_time_now)))

        self.balance = 0
        self.last_long_transaction_price = 0
        self.last_short_transaction_price = 0
        self.commission = 0
        self.candles = []
        self.strategies = []

    def __init__(self):
        self.name = "simulator"
        self.__init__(self.name)

    def add_strategy(self, strategy: Strategy):
        self.strategies.append(strategy)

    def add_candle_dataframe(self, df: DataFrame):
        required_columns = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        validate_dataframe_columns(required_columns)
        df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].astype(float)
        df[['volume']] = df[['volume']].astype(int)

        for index, row in df.iterrows():
            candle = Candle(
                row['symbol'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume'],
                row['timestamp']
            )
            self.candles.append(candle)

    def add_candle_data_from_db(self, symbol: str, timeframe: pd.offsets.DateOffset, start: datetime,
                                end: datetime = datetime.now()):
        df = CandleFetcher.fetch(symbol, timeframe, start, end)
        self.add_candle_dataframe(df)

    def add_candle_csv_data(self, csv_file_path):
        df = pd.read_csv('data.csv')
        self.add_candle_dataframe(df)

    def fund(self, balance: float):
        self.balance += balance

    def set_commission(self, commission):
        self.commission = commission

    def getActions(self, strategy: Strategy, candles: list) -> list:
        actions = []
        for candle in candles:
            action = strategy.compute_action(candle)
            actions.append(action)
        return actions

    def runSimulation(self, strategy: Strategy, candles: list):
        logger.info("Starting funds: {}".format(self.balance))
        actions = self.getActions(strategy, candles)
        for action in actions:
            if action.position_type == PositionType.LONG:
                if action.action_type == ActionType.BUY:
                    unit_cost_estimate = action.market_price * (1 + self.commission)
                    logger.info("BUY LONG at {}".format(unit_cost_estimate))
                    self.last_long_transaction_price = unit_cost_estimate
                elif action.action_type == ActionType.SELL:
                    unit_cost_estimate = action.market_price * (1 + self.commission)
                    logger.info("SELL LONG at {}".format(unit_cost_estimate))
                    profit = unit_cost_estimate - self.last_long_transaction_price
                    logger.info("TRANSACTION PROFIT {}".format(profit))
                    self.balance += profit
                    self.last_long_transaction_price = 0
            elif action.position_type == PositionType.SHORT:
                if action.action_type == ActionType.BUY:
                    unit_cost_estimate = action.market_price * (1 + self.commission)
                    logger.info("SELL LONG at {}".format(unit_cost_estimate))
                    self.last_long_transaction_price = unit_cost_estimate
                elif action.action_type == ActionType.SELL:
                    unit_cost_estimate = action.market_price * (1 + self.commission)
                    logger.info("BUY LONG at {}".format(unit_cost_estimate))
                    profit = self.last_long_transaction_price - unit_cost_estimate
                    logger.info("TRANSACTION PROFIT {}".format(profit))
                    self.balance += profit
                    self.last_long_transaction_price = 0

        logger.info("Final funds: {}".format(self.balance))

    def runSimulation(self):
        for strategy in self.strategies:
            self.runSimulation(strategy, self.candles)


if __name__ == "__main__":
    pass
