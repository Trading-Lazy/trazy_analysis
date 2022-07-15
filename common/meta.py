import inspect
import uuid
from abc import ABCMeta
from typing import Any

from ratelimit import limits, sleep_and_retry


class InheritableSingleton(ABCMeta):
    instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            cls.instances[cls] = super().__call__(*args, **kwargs)
        return cls.instances[cls]


class RateLimitedSingleton(InheritableSingleton):
    instances = {}

    def __init__(cls, cls_name, bases, namespace, **kwds):
        if not isinstance(cls.MAX_CALLS, property) and not isinstance(
            cls.PERIOD, property
        ):
            from trazy_analysis.common.helper import request

            setattr(
                cls,
                "request",
                sleep_and_retry(
                    limits(calls=cls.MAX_CALLS, period=cls.PERIOD)(request)
                ),
            )
        super().__init__(cls_name, bases, namespace, **kwds)


class SpecialParameter:
    def __init__(
        self, name: str, value_type: type, default_value: Any, value: Any = None
    ):
        self.name = name
        self.value_type = value_type
        if not isinstance(default_value, self.value_type) or (
            value is not None and not isinstance(default_value, self.value_type)
        ):
            raise Exception(
                f"default value and value should be of type {self.value_type}"
            )
        self.default_value = default_value
        self.value = value

    def set_value(self, value: Any):
        if value is None:
            return
        if not isinstance(value, self.value_type):
            raise Exception(
                f"value should be of type {self.value_type} but is instead {type(value)}"
            )
        self.value = value


class IndicatorMemoization(type):
    def __new__(mcs, name, bases, attrs):
        if "_instances" not in attrs:
            attrs["_instances"] = dict()
        return type.__new__(mcs, name, bases, attrs)

    def __call__(cls, *args, **kwargs):
        key_args = list(args)
        key_kwargs = kwargs.copy()
        signature = inspect.signature(cls.__init__)
        parameters = list(signature.parameters.keys())
        special_parameters = {
            "size": SpecialParameter(name="size", value_type=int, default_value=5),
            "memoize": SpecialParameter(
                name="memoize", value_type=bool, default_value=True
            ),
        }
        indexes_to_remove = []
        for special_parameter in special_parameters.values():
            if special_parameter.name in signature.parameters:
                special_parameter_index = parameters.index(special_parameter.name) - 1
                if special_parameter_index >= len(args):
                    if special_parameter.name in key_kwargs:
                        special_parameter.set_value(
                            key_kwargs.pop(special_parameter.name)
                        )
                else:
                    special_parameter.set_value(
                        special_parameter.value_type(args[special_parameter_index])
                    )
                    indexes_to_remove.append(special_parameter_index)
            else:
                special_parameter.set_value(special_parameter.default_value)
        if indexes_to_remove:
            key_args = [
                arg
                for index, arg in enumerate(key_args)
                if index not in indexes_to_remove
            ]

        if special_parameters["memoize"].value:
            key = [cls.__name__]
            key.extend(map(id, key_args))
            if key_kwargs:
                for k, v in key_kwargs.items():
                    key.append(id(k))
                    key.append(id(v))
            key = tuple(key)
        else:
            key = uuid.uuid4()

        if key not in cls._instances:
            cls._instances[key] = super().__call__(*args, **kwargs)
        return cls._instances[key]
