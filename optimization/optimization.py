from typing import Dict

import numpy as np

from trazy_analysis.common.backtest import Backtest
from trazy_analysis.models.parameter import Parameter
from trazy_analysis.optimization.optimizer import Optimizer


class Optimization:
    def __init__(
        self,
        backtest: Backtest,
        optimizer: Optimizer,
        parameters_spaces: dict[type, dict[str, Parameter]] = {},
        nb_iter: int = 54,
        max_evals: int = 1,
    ):
        def run_strategy(strategy_class, kwargs):
            result = backtest.run_strategy(strategy_class, kwargs).loc["Sortino Ratio"][
                "Backtest results"
            ]
            return result if not np.isnan(result) else -1

        for strategy_class, parameters_space in parameters_spaces.items():
            best_params_dict = optimizer.maximize(
                lambda **kwargs: run_strategy(strategy_class, kwargs),
                parameters_space,
                nb_iter=nb_iter,
                max_evals=max_evals,
            )
            print(best_params_dict)
