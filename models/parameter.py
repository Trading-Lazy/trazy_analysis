from typing import Any, TypeVar

import numpy as np

rng = np.random.RandomState(None)

TParameter = TypeVar("TParameter", bound="Parameter")

class Parameter(object):
    """
    Defines a hyperparameter with a name, type and associated range.
    Args:
        range (list): either ``[low, high]`` or ``[value1, value2, value3]``.
        scale (str): `linear` or `log`, defines sampling from linear or
            log-scale. Not defined for all parameter types.
    """

    def __init__(self, range: list) -> None:
        assert isinstance(range, list), "Parameter-Range needs to be a list."
        self.range = range

    @staticmethod
    def from_dict(parameter_dict: dict[str, Any]) -> TParameter:
        """
        Returns a parameter object according to the given dictionary config.
        Args:
            parameter_dict (dict): parameter config.
        Example:
        ::
            {'type': '<continuous/discrete/choice>',
             'range': [<value1>, <value2>, ... ],
             'scale': <'log' to sample continuous/discrete from log-scale>}
        Returns:
            sherpa.core.Parameter: the parameter range object.
        """
        parameter_type = parameter_dict.get("type")
        match parameter_type:
            case "continuous":
                return Continuous(
                    range=parameter_dict.get("range"),
                    scale=parameter_dict.get("scale", "linear"),
                )
            case "discrete":
                return Discrete(
                    range=parameter_dict.get("range"),
                    scale=parameter_dict.get("scale", "linear"),
                )
            case "choice":
                return Choice(range=parameter_dict.get("range"))
            case "ordinal":
                return Ordinal(range=parameter_dict.get("range"))
            case "static":
                return Static(value=parameter_dict.get("range")[0])
            case _:
                raise ValueError(
                    "Got unexpected value for type: {}".format(parameter_dict.get("type"))
                )


class Continuous(Parameter):
    """
    Continuous parameter class.
    """

    def __init__(self, range: list, scale: str = "linear") -> None:
        super().__init__(range)
        self.scale = scale
        if scale == "log":
            assert all(r > 0.0 for r in range), (
                "Range parameters must be " "positive for log scale."
            )


class Discrete(Parameter):
    """
    Discrete parameter class.
    """

    def __init__(self, range: list, scale: str = "linear") -> None:
        super().__init__(range)
        self.scale = scale
        if scale == "log":
            assert all(r > 0 for r in range), (
                "Range parameters must be " "positive for log scale."
            )


class Choice(Parameter):
    """
    Choice parameter class.
    """

    def __init__(self, range: list) -> None:
        super().__init__(range)


class Ordinal(Parameter):
    """
    Ordinal parameter class. Categorical, ordered variable.
    """

    def __init__(self, range: list) -> None:
        super().__init__(range)
        self.type = type(self.range[0])


class Static(Parameter):
    """
    Static parameter class. A fixed parameter.
    """

    def __init__(self, value: Any) -> None:
        super().__init__([value])
        self.type = type(self.range[0])
