from abc import ABC, abstractmethod
from collections import Callable

from typing import Any, Dict

from trazy_analysis.optimization.optimizer import Optimizer
from trazy_analysis.optimization.parameter import Parameter


class BlackBoxOptimizer(Optimizer):
    @abstractmethod
    def maximize(
        self,
        func: Callable,
        space: Dict[str, Parameter],
        nb_iter: int = 54,
        max_evals: int = 1,
    ) -> Dict[str, Any]:
        raise NotImplementedError("Should implement maximize()")

