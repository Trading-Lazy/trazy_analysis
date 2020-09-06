import multiprocessing
from decimal import Decimal
from multiprocessing import Queue
from typing import List

import pandas as pd
from pandas import DataFrame
from pandas_market_calendars import MarketCalendar

from common.utils import validate_dataframe_columns
from db_storage.db_storage import DbStorage
from file_storage.file_storage import FileStorage
from models.candle import Candle
from simulator.simulation import Simulation
from strategy.candlefetcher import CandleFetcher
from strategy.strategy import Strategy


class Simulator:
    def __init__(self, db_storage: DbStorage, symbol: str = "symbol"):
        self.db_storage = db_storage
        self.cash = 0
        self.commission = 0
        self.candles = []
        self.simulations = []
        self.symbol = symbol

    def add_strategy(self, strategy: Strategy, log: bool = True):
        simulation = Simulation(
            strategy,
            self.db_storage,
            self.candles,
            self.cash,
            self.commission,
            self.symbol,
            log,
        )
        self.simulations.append(simulation)

    def add_candles_from_dataframe(self, df: DataFrame):
        required_columns = [
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "timestamp",
        ]
        validate_dataframe_columns(df, required_columns)
        self.candles = []
        self.symbol = df[["symbol"]].iloc[0]
        df[["volume"]] = df[["volume"]].astype(int)

        for row in df.itertuples():
            candle = Candle(
                symbol=row.symbol,
                open=Decimal(row.open),
                high=Decimal(row.high),
                low=Decimal(row.low),
                close=Decimal(row.close),
                volume=row.volume,
                timestamp=pd.Timestamp(row.timestamp, tz="UTC"),
            )
            self.candles.append(candle)

    def add_candles_from_db(
        self,
        db_storage: DbStorage,
        file_storage: FileStorage,
        market_cal: MarketCalendar,
        symbol: str,
        start: pd.Timestamp,
        end: pd.Timestamp = pd.Timestamp.now(tz="UTC"),
    ):
        self.symbol = symbol
        candles = CandleFetcher(db_storage, file_storage, market_cal).query_candles(
            symbol, start, end
        )
        self.set_candles(candles)

    def add_candles_from_csv(self, csv_file_path, sep=","):
        self.candles = []
        dtype = {
            "symbol": str,
            "open": str,
            "high": str,
            "low": str,
            "close": str,
            "volume": int,
            "timestamp": str,
        }
        df = pd.read_csv(csv_file_path, dtype=dtype, sep=sep)
        self.add_candles_from_dataframe(df)

    def set_candles(self, candles: List[Candle]):
        self.candles = candles
        for simulation in self.simulations:
            simulation.candles = candles

    def fund(self, cash: Decimal):
        self.cash += cash
        for simulation in self.simulations:
            simulation.fund(self.cash)

    def set_commission(self, commission: Decimal):
        self.commission = commission
        for simulation in self.simulations:
            simulation.commission = self.commission

    def run_simulation(self, simulation: Simulation, final_states: Queue):
        simulation.run()
        final_states.put(
            {
                "strategy_name": simulation.strategy.name,
                "shares_amounts": simulation.shares_amounts,
                "portfolio_value": simulation.portfolio_value,
                "cash": simulation.cash,
            }
        )

    def run(self) -> Queue():
        processes = []
        final_states = Queue()
        for simulation in self.simulations:
            p = multiprocessing.Process(
                target=self.run_simulation, args=[simulation, final_states]
            )
            p.start()
            processes.append(p)
        for p in processes:
            p.join()
        return final_states
