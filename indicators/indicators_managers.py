import glob
import importlib
import inspect
import os
import uuid
from pathlib import Path
from typing import Callable, Set, Tuple

import numpy as np
import pandas as pd

from trazy_analysis.indicators.indicator import Indicator
from trazy_analysis.models.enums import ExecutionMode


def get_module_classes(module_name: str, module_path: str) -> Set[type]:
    classes = set()
    for file in glob.glob(str(module_path) + "/*.py"):
        name = os.path.splitext(os.path.basename(file))[0]
        # Ignore __ files
        if name.startswith("__"):
            continue
        module = importlib.import_module(module_name + "." + name, package=None)

        for member in dir(module):
            indicator_class = getattr(module, member)
            if (
                indicator_class
                and inspect.isclass(indicator_class)
                and issubclass(indicator_class, Indicator)
            ):
                classes.add(indicator_class)
    return classes


current_path = Path(__file__)
module_path = current_path.parent
MODULE_NAME = "trazy_analysis.indicators"
indicators_classes = get_module_classes(MODULE_NAME, str(module_path))


class ReactiveIndicators:
    def __init__(self, memoize: bool = True, mode: ExecutionMode = ExecutionMode.LIVE):
        self.memoize = memoize
        self.mode = mode

        for class_to_enrich in indicators_classes:

            def indicator_call(
                indicator_class: type,
            ) -> Callable:
                def indicator_call_helper(*args, **kwargs) -> Indicator:
                    if not hasattr(indicator_class, "_instances"):
                        indicator_class._instances = {}
                    if not hasattr(indicator_class, "_max_sizes"):
                        indicator_class._max_sizes = {}
                    if self.memoize:
                        key = [id(self), indicator_class.__name__]
                        key.extend(map(id, args))
                        if kwargs:
                            for k, v in kwargs.items():
                                key.append(id(k))
                                key.append(id(v))
                        key = tuple(key)
                    else:
                        key = uuid.uuid4()

                    if key not in indicator_class._instances:
                        indicator_class._instances[key] = indicator_class(
                            *args, **kwargs
                        )
                        indicator_class._instances[key].setup(self)
                    return indicator_class._instances[key]

                return indicator_call_helper

            setattr(
                self,
                class_to_enrich.__name__,
                indicator_call(class_to_enrich),
            )


class Indicators:
    pass


for indicators_class in indicators_classes:

    def compute(compute_class: Indicator):
        def compute_helper(*args, **kwargs):
            data = None
            if args:
                data = args[0]
                args = list(args)[1:]
            elif kwargs:
                data = kwargs.pop("data", None)
            if data is None:
                raise Exception("There should be a data parameter for the indicator.")
            elif not isinstance(data, np.ndarray) and not isinstance(
                data, pd.DataFrame
            ):
                raise Exception("data should be a numpy array or a pandas dataframe")
            if isinstance(data, pd.DataFrame):
                data = data.to_numpy()
                shape = list(data.shape)
                while len(shape) > 1 and shape[-1] == 1:
                    data = data.flatten()
                    shape = list(data.shape)

            return compute_class.compute(data, *args, **kwargs)

        return compute_helper

    setattr(Indicators, indicators_class.__name__, compute(indicators_class))
