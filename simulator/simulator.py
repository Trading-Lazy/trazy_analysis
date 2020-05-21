from datetime import datetime
from decimal import Decimal
from multiprocessing import Queue
from typing import List

import pandas as pd
from pandas import DataFrame

import multiprocessing
from simulator.simulation import Simulation
from strategy.strategy import Strategy
from actionsapi.models import Candle
from strategy.constants import DATE_FORMAT
from common.utils import validate_dataframe_columns


class Simulator:
    def __init__(self, symbol: str = "symbol"):
        self.cash = 0
        self.commission = 0
        self.candles = []
        self.simulations = []
        self.symbol = symbol

    def add_strategy(self, strategy: Strategy, log: bool = True):
        simulation = Simulation(
            strategy, self.candles, self.cash, self.commission, self.symbol, log
        )
        self.simulations.append(simulation)

    def add_candles_from_dataframe(self, df: DataFrame):
        required_columns = [
            "id",
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
        df[["id", "volume"]] = df[["id", "volume"]].astype(int)

        for index, row in df.iterrows():
            candle = Candle(
                id=row["id"],
                symbol=row["symbol"],
                open=Decimal(row["open"]),
                high=Decimal(row["high"]),
                low=Decimal(row["low"]),
                close=Decimal(row["close"]),
                volume=row["volume"],
                timestamp=datetime.strptime(row["timestamp"], DATE_FORMAT),
            )
            self.candles.append(candle)

    def add_candles_data_from_db(
        self, symbol: str, start: datetime, end: datetime = datetime.now()
    ):
        self.symbol = symbol
        db_candles = Candle.objects.all().filter(
            symbol=symbol, timestamp__gte=start, timestamp__lte=end
        )
        candles = []
        for db_candle in db_candles:
            candles.append(db_candle)
        self.set_candles(candles)

    def add_candles_csv_data(self, csv_file_path, sep=","):
        self.candles = []
        dtype = {
            "id": int,
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
