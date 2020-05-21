import abc
import os
import settings
from enum import Enum, auto
from .action import Action, PositionType, ActionType
from common.candle import *
from common.helper import TimeInterval, find_start_business_minute, find_start_business_day
from .candlefetcher import CandleFetcher
import pandas as pd
from pymongo import MongoClient
import logger
import datetime
from .ta import ma
import numpy as np
from common.exchange_calendar_euronext import EuronextExchangeCalendar


LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, 'output.log'))

m_client = MongoClient(settings.DB_CONN)
db = m_client['djongo_connection']
euronext_cal = EuronextExchangeCalendar()


class StrategyName(Enum):
    SMA_CROSSOVER = auto()


class Strategy:
    def __init__(self, name: str):
        self.is_opened = False
        self.name = name
        self.__parameters = self.init_default_parameters()

    @abc.abstractmethod
    def compute_action(self, candle) -> Action:
        raise NotImplementedError

    @abc.abstractmethod
    def init_default_parameters(self):
        raise NotImplementedError

    def process_candle(self, candle: Candle):
        action = self.compute_action(candle)
        if not action is None:
            action.save()


class SmaCrossoverStrategy(Strategy):
    def __init__(self):
        super().__init__(StrategyName.SMA_CROSSOVER.name)
        self.__interval: TimeInterval = None
        self.__short_period = None
        self.__long_period = None
        self.__parameters = {}
        self.init_default_parameters()

    def init_default_parameters(self):
        self.__interval: TimeInterval = TimeInterval.process_interval('1 day')
        self.__short_period = 3
        self.__long_period = 8
        self.__parameters = {
            'interval': self.__interval,
            'short_period': self.__short_period,
            'long_period': self.__long_period
        }

    def set_interval(self, interval: str):
        self.__interval = TimeInterval.process_interval(interval)
        self.__parameters['interval'] = self.__interval

    def set_short_period(self, short_period):
        self.__short_period = short_period
        self.__parameters['short_period'] = self.__short_period

    def set_long_period(self, long_period):
        self.__long_period = long_period
        self.__parameters['long_period'] = self.__long_period

    def get_interval(self) -> TimeInterval:
        return self.__interval

    def get_short_period(self) -> int:
        return self.__short_period

    def get_long_period(self) -> int:
        return self.__long_period

    def get_parameters(self) -> dict:
        return self.__parameters

    def set_parameters(self, param: dict):
        if 'interval' in param:
            self.__interval = TimeInterval.process_interval(param['interval'])
            self.__parameters['interval'] = self.__interval

        if 'short_period' in param:
            self.__short_period = param['short_period']
            self.__parameters['short_period'] = self.__short_period

        if 'long_period' in param:
            self.__long_period = param['long_period']
            self.__parameters['long_period'] = self.__long_period

    def get_candles_with_signals_positions(self, history_candles:pd.DataFrame) -> pd.DataFrame:
        if history_candles.shape[0] < (self.__long_period + 1):
            raise Exception("There is not ")
        df = ma(history_candles, self.__long_period)
        df = ma(df, self.__short_period)
        df['signals'] = np.where(
            df['MA_{}'.format(self.__short_period)] > df['MA_{}'.format(self.__long_period)], 1.0, 0.0)
        df['positions'] = df['signals'].diff()
        return df

    def get_time_offset(self):
        if self.__interval.interval_unit == 'day':
            offset = pd.offsets.Day(self.__interval.interval_value)
        elif self.__interval.interval_unit == 'minute':
            offset = pd.offsets.Minute(self.__interval.interval_value)
        else:
            raise Exception('Tne unit of time interval: {} is not recognized by {}'.format(self.__interval.interval_unit,
                                                                                            self.name))
        return offset

    def calc_required_history_start_timestamp(self, end_timestamp: datetime) -> datetime:
        if self.__interval.interval_unit == 'day':
            start_timestamp = find_start_business_day(end_timestamp,
                                                      euronext_cal,
                                                      self.__interval,
                                                      self.__long_period + 1)

        elif self.__interval.interval_unit == 'minute':
            start_timestamp = find_start_business_minute(end_timestamp,
                                                         euronext_cal,
                                                         self.__interval,
                                                         self.__long_period)
        else:
            raise Exception('Tne unit of time interval: {} is not recognized by {}'.format(self.__interval.interval_unit,
                                                                                           self.name))
        return start_timestamp

    @staticmethod
    def conclude_action_position(df_positions: pd.DataFrame) -> (ActionType, PositionType):
        position = df_positions.iloc[-1]['positions']
        computed_action = None
        computed_position = None
        if position == 1:
            computed_action = ActionType.BUY
            computed_position = PositionType.LONG
        elif position == -1:
            computed_action = ActionType.SELL
            computed_position = PositionType.LONG
        return computed_action, computed_position

    @staticmethod
    def build_action(candle, action, position):
        if not action is None:
            return Action(strategy=StrategyName.SMA_CROSSOVER.name,
                          symbol=candle.symbol,
                          confidence_level=1,
                          action_type=action,
                          position_type=position)
        else:
            return Action(strategy=StrategyName.SMA_CROSSOVER.name,
                          symbol=candle.symbol,
                          confidence_level=1,
                          action_type=ActionType.BUY,
                          position_type=PositionType.LONG)

    def calc_strategy(self, candle: Candle, history_candles: pd.DataFrame) -> Action:
        df_sma = self.get_candles_with_signals_positions(history_candles)
        computed_action, computed_position = SmaCrossoverStrategy.conclude_action_position(df_sma)
        return SmaCrossoverStrategy.build_action(candle,computed_action,computed_position)

    def compute_action(self, candle: Candle) -> Action:
        # fetch action
        last_action = db['actionsapi_action'].find_one(filter={"strategy": StrategyName.SMA_CROSSOVER.name},
                                                        sort=[("id", -1)])
        LOG.info("LAST ACTION : {}".format(last_action))

        # fetch candles
        time_now = datetime.datetime.now()

        offset = self.get_time_offset()
        start_timestamp = self.calc_required_history_start_timestamp(time_now)

        df_hist = CandleFetcher.fetch(candle.symbol, offset, euronext_cal, pd.to_datetime(start_timestamp))

        if not df_hist.empty:
            action = self.calc_strategy(candle, df_hist)
            if last_action['action_type'] == action.action_type.name:
                return None
            return action
        else:
            LOG.info("No history candles can be used to calculate the strategy")
            return None

