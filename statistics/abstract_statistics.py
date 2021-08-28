from abc import ABCMeta, abstractmethod

import pandas as pd


class AbstractStatistics(object):
    """
    Statistics is an abstract class providing an interface for
    all inherited statistic classes (live, historic, custom, etc).
    The goal of a Statistics object is to keep a record of useful
    information about one or many trading strategies as the strategy
    is running. This is done by hooking into the main event loop and
    essentially updating the object according to portfolio performance
    over time.
    Ideally, Statistics should be subclassed according to the strategies
    and timeframes-traded by the user. Different trading strategies
    may require different metrics or frequencies-of-metrics to be updated,
    however the example given is suitable for longer timeframes.
    """

    __metaclass__ = ABCMeta

    def __init__(
        self,
        equity: pd.DataFrame,
        benchmark_equity: pd.DataFrame = None,
        positions: pd.DataFrame = None,
        transactions: pd.DataFrame = None,
        title=None,
    ):
        self.equity = equity
        self.returns = equity["Equity"].pct_change().fillna(0.0)
        self.positions = positions
        self.transactions = transactions
        self.benchmark_equity = benchmark_equity
        self.benchmark_returns = (
            benchmark_equity["Equity"].pct_change().fillna(0.0)
            if benchmark_equity is not None
            else None
        )
        self.title = title

    @abstractmethod
    def get_tearsheet(self) -> pd.DataFrame:
        """
        Return a dataframe containing all statistics.
        """
        raise NotImplementedError("Should implement get_results()")

    @abstractmethod
    def plot_tearsheet(self) -> None:
        """
        Plot all statistics collected
        """
        raise NotImplementedError("Should implement plot_results()")

    def save_tearsheet(self, filename: str) -> None:
        """
        Save statistics results to filename
        """
        tearsheet_df = self.get_tearsheet()
        tearsheet_df.to_csv(filename)
