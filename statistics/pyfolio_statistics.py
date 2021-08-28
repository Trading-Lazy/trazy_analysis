import pandas as pd
import pyfolio as pf

from trazy_analysis.statistics.abstract_statistics import AbstractStatistics


class PyfolioStatistics(AbstractStatistics):
    """
    Displays a Pyfolio 'one-pager' performance report.
    """

    def __init__(
        self,
        equity: pd.DataFrame,
        positions: pd.DataFrame = None,
        transactions: pd.DataFrame = None,
        benchmark_equity=None,
        title=None,
    ):
        super().__init__(
            equity=equity,
            benchmark_equity=benchmark_equity,
            positions=positions,
            transactions=transactions,
            title=title,
        )

    def get_tearsheet(self) -> pd.DataFrame:
        """
        Return a dataframe containing all statistics.
        """
        return pf.timeseries.perf_stats(
            returns=self.returns,
            factor_returns=self.benchmark_returns,
            positions=self.positions,
            transactions=self.transactions,
        ).to_frame(name="Backtest results")

    def plot_tearsheet(self) -> None:
        """
        Plot all statistics collected up until 'now'
        """
        pf.create_full_tear_sheet(
            returns=self.returns,
            benchmark_rets=self.benchmark_returns,
            positions=self.positions,
            transactions=self.transactions,
            estimate_intraday=True,
        )
