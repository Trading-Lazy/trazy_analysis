from decimal import Decimal

import numpy as np
import pandas as pd
from common.helper import (
    TimeInterval,
    calc_required_history_start_timestamp,
)
from db_storage.db_storage import DbStorage
from models.signal import Signal
from models.candle import Candle
from models.enums import Action, Direction
from models.signal import Signal
from order_manager.order_management import OrderManager
from strategy.candlefetcher import CandleFetcher
from strategy.strategy import LOG, Strategy, euronext_cal
from strategy.ta import ma


class SmaCrossoverStrategy(Strategy):
    def __init__(
        self, symbol: str, db_storage: DbStorage, order_manager: OrderManager
    ):
        super().__init__(symbol, db_storage, order_manager)
        self.__interval: TimeInterval = None
        self.__short_period = None
        self.__long_period = None
        self.__parameters = {}
        self.candle_fetcher = CandleFetcher(
            db_storage=self.db_storage,
            file_storage=self.file_storage,
            market_cal=euronext_cal,
        )
        self.init_default_parameters()

    def init_default_parameters(self):
        self.__interval: TimeInterval = TimeInterval.process_interval("1 day")
        self.__short_period = 3
        self.__long_period = 8
        self.__parameters = {
            "interval": self.__interval,
            "short_period": self.__short_period,
            "long_period": self.__long_period,
        }

    def set_interval(self, interval: str):
        self.__interval = TimeInterval.process_interval(interval)
        self.__parameters["interval"] = self.__interval

    def set_short_period(self, short_period):
        self.__short_period = short_period
        self.__parameters["short_period"] = self.__short_period

    def set_long_period(self, long_period):
        self.__long_period = long_period
        self.__parameters["long_period"] = self.__long_period

    def get_interval(self) -> TimeInterval:
        return self.__interval

    def get_short_period(self) -> int:
        return self.__short_period

    def get_long_period(self) -> int:
        return self.__long_period

    def get_parameters(self) -> dict:
        return self.__parameters

    def get_parameters_json(self) -> dict:
        return {
            "interval_unit": self.__interval.interval_unit,
            "interval_value": self.__interval.interval_value,
            "short_period": self.__short_period,
            "long_period": self.__long_period,
        }

    def set_parameters(self, param: dict):
        if "interval" in param:
            self.__interval = TimeInterval.process_interval(param["interval"])
            self.__parameters["interval"] = self.__interval

        if "short_period" in param:
            self.__short_period = param["short_period"]
            self.__parameters["short_period"] = self.__short_period

        if "long_period" in param:
            self.__long_period = param["long_period"]
            self.__parameters["long_period"] = self.__long_period

    def get_candles_with_signals_positions(
        self, history_candles: pd.DataFrame
    ) -> pd.DataFrame:
        if history_candles.shape[0] < (self.__long_period + 1):
            raise Exception("There is not enough historical data to run this strategy")
        df = ma(history_candles, self.__long_period)
        df = ma(df, self.__short_period)
        df["signals"] = np.where(
            df["MA_{}".format(self.__short_period)]
            > df["MA_{}".format(self.__long_period)],
            1.0,
            0.0,
        )
        df["positions"] = df["signals"].diff()
        return df

    def get_time_offset(self):
        if self.__interval.interval_unit == "day":
            offset = pd.offsets.Day(self.__interval.interval_value)
        elif self.__interval.interval_unit == "minute":
            offset = pd.offsets.Minute(self.__interval.interval_value)
        else:
            raise Exception(
                "Tne unit of time interval: {} is not recognized by {}".format(
                    self.__interval.interval_unit, self.name
                )
            )
        return offset

    @staticmethod
    def conclude_action_position(
        df_positions: pd.DataFrame,
    ) -> (Action, Direction):
        position = df_positions.iloc[-1]["positions"]
        generated_action = None
        generated_direction = None
        if position == 1:
            generated_action = Action.BUY
            generated_direction = Direction.LONG
        elif position == -1:
            generated_action = Action.SELL
            generated_direction = Direction.LONG
        return generated_action, generated_direction

    def build_signal(self, candle: Candle, action: Action, direction: Direction):
        if action is not None:
            return Signal(
                action=action,
                direction=direction,
                confidence_level=Decimal("1.0"),
                strategy=self.name,
                symbol=candle.symbol,
                root_candle_timestamp=candle.timestamp,
                parameters={},
            )

        return None

    def calc_strategy(self, candle: Candle, history_candles: pd.DataFrame) -> Signal:
        df_sma = self.get_candles_with_signals_positions(history_candles)
        (
            generated_action,
            generated_direction,
        ) = SmaCrossoverStrategy.conclude_action_position(df_sma)
        return self.build_signal(candle, generated_action, generated_direction)

    def get_last_signal(self) -> Signal:
        signals = self.db_storage.get_all_signals()
        signals = sorted(signals, key=lambda signal: signal.timestamp, reverse=True)
        return signals[-1]

    def generate_signal(self, candle: Candle) -> Signal:
        # fetch signal
        last_signal: Signal = self.get_last_signal()
        self.__last_candle_timestamp = candle.timestamp

        LOG.info("LAST SIGNAL : {}".format(last_signal))

        # fetch candles
        offset = self.get_time_offset()
        start_timestamp = calc_required_history_start_timestamp(
            offset, self.__long_period, euronext_cal, candle.timestamp
        )

        df_hist = self.candle_fetcher.fetch(
            candle.symbol, offset, start=start_timestamp, end=candle.timestamp
        )

        if not df_hist.empty:
            signal = self.calc_strategy(candle, df_hist)
            if last_signal is not None and last_signal.action == signal.action.name:
                return None

            return signal
        else:
            LOG.info("No history candles can be used to calculate the strategy")
            return None
