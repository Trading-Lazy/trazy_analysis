import pandas as pd
import quantstats as qs

from trazy_analysis.statistics.abstract_statistics import AbstractStatistics


class QuantstatsStatistics(AbstractStatistics):
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
        return qs.reports.metrics(
            returns=self.returns, benchmark_returns=self.benchmark_returns, mode="full"
        )

    def plot_tearsheet(self) -> None:
        """
        Plot all statistics collected up until 'now'
        """
        qs.reports.plots(
            returns=self.returns, benchmark=self.benchmark_returns, mode="full"
        )
