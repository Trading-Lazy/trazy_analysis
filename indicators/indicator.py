import operator
from collections import deque
from decimal import Decimal
from typing import Any, Callable, List, Union

import numpy as np

from common.helper import check_type


class Indicator:
    instances = 0
    count = 0

    def __init__(
        self,
        source_indicator: "Indicator" = None,
        transform: Callable = lambda new_data: new_data,
    ):
        Indicator.instances += 1
        self.transform = transform
        self.source_indicator = source_indicator
        self.data = None
        self.callback = lambda new_elt: self.handle_new_data(new_elt)
        self.callbacks = deque()
        self.observe(self.source_indicator)

    def subscribe(self, callback):
        self.callbacks.append(callback)

    def on_next(self, value: Any):
        for callback in self.callbacks:
            callback(value)

    def handle_new_data(self, new_data: Any) -> None:
        Indicator.count += 1
        transformed_data = self.transform(new_data)
        self.data = transformed_data
        self.on_next(transformed_data)

    def observe(self, indicator_data: "Indicator"):
        self.ignore()
        self.source_indicator = indicator_data
        if self.source_indicator is not None:
            indicator_data.subscribe(self.callback)
            if (
                isinstance(indicator_data, Indicator)
                and self.source_indicator.data is not None
            ):
                self.data = self.transform(self.source_indicator.data)

    def remove_callback(self, callback):
        try:
            self.callbacks.remove(callback)
        except ValueError:
            pass

    def ignore(self):
        if self.source_indicator is not None:
            self.source_indicator.remove_callback(self.callback)

    def indicator_unary_operation(
        self,
        operation_function: Callable[[Any], Any],
        allowed_types: List[type],
    ) -> "Indicator":
        check_type(self.data, allowed_types)
        indicator_data: Indicator = Indicator(
            source_indicator=self, transform=operation_function
        )
        return indicator_data

    def indicator_binary_operation_indicator(
        self, other: "Indicator", operation_function: Callable[[Any, Any], Any]
    ) -> "Indicator":
        indicator_zip_data = ZipIndicator(
            source_indicator1=self,
            source_indicator2=other,
            operation_function=operation_function,
        )
        return indicator_zip_data

    def indicator_binary_operation_data(
        self, other, operation_function: Callable[[Any, Any], Any]
    ) -> "Indicator":
        transform = lambda new_data: (operation_function(new_data, other))
        indicator_data: Indicator = Indicator(
            source_indicator=self, transform=transform
        )
        return indicator_data

    def indicator_binary_operation(
        self,
        other,
        operation_function: Callable[[Any, Any], Any],
        allowed_types: List[type],
    ) -> "Indicator":
        check_type(self.data, allowed_types)

        other_is_indicator = isinstance(other, Indicator)
        other_data = other.data if other_is_indicator else other
        check_type(other_data, allowed_types)

        if other_is_indicator:
            return self.indicator_binary_operation_indicator(other, operation_function)
        else:
            return self.indicator_binary_operation_data(other, operation_function)

    def unary_operation(
        self,
        operation_function: Callable[[Any], Any],
        allowed_types: List[type],
    ) -> "Indicator":
        if self.data is None:
            return None
        check_type(self.data, allowed_types)
        return operation_function(self.data)

    def binary_operation(
        self,
        other,
        operation_function: Callable[[Any, Any], Any],
        allowed_types: List[type],
    ) -> Union[int, bool, float, Decimal]:
        if self.data is None:
            return None
        check_type(self.data, allowed_types)
        other_is_indicator = isinstance(other, Indicator)
        other_data = other.data if other_is_indicator else other
        if other_data is None:
            return None
        check_type(other_data, allowed_types)

        if other_is_indicator:
            return operation_function(self.data, other.data)
        else:
            return operation_function(self.data, other)

    def __lt__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__lt__, [int, float, np.float64, Decimal]
        )

    def __le__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__le__, [int, float, np.float64, Decimal]
        )

    def __eq__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__eq__, [int, float, np.float64, Decimal]
        )

    def __ne__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__ne__, [int, float, np.float64, Decimal]
        )

    def __ge__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__ge__, [int, float, np.float64, Decimal]
        )

    def __gt__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__gt__, [int, float, np.float64, Decimal]
        )

    def __add__(self, decimal_or_indicator) -> Union[int, float, np.float64, Decimal]:
        return self.binary_operation(
            decimal_or_indicator, operator.__add__, [int, float, np.float64, Decimal]
        )

    def __iadd__(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__iadd__, [int, float, np.float64, Decimal]
        )

    def __sub__(self, decimal_or_indicator) -> Union[int, float, np.float64, Decimal]:
        return self.binary_operation(
            decimal_or_indicator, operator.__sub__, [int, float, np.float64, Decimal]
        )

    def __isub__(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__isub__, [int, float, np.float64, Decimal]
        )

    def __mul__(self, decimal_or_indicator) -> Union[int, float, np.float64, Decimal]:
        return self.binary_operation(
            decimal_or_indicator, operator.__mul__, [int, float, np.float64, Decimal]
        )

    def __imul__(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__imul__, [int, float, np.float64, Decimal]
        )

    def __truediv__(
        self, decimal_or_indicator
    ) -> Union[int, float, np.float64, Decimal]:
        return self.binary_operation(
            decimal_or_indicator,
            operator.__truediv__,
            [int, float, np.float64, Decimal],
        )

    def __itruediv__(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator,
            operator.__itruediv__,
            [int, float, np.float64, Decimal],
        )

    def __floordiv__(
        self, decimal_or_indicator
    ) -> Union[int, float, np.float64, Decimal]:
        return self.binary_operation(
            decimal_or_indicator,
            operator.__floordiv__,
            [int, float, np.float64, Decimal],
        )

    def __ifloordiv__(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator,
            operator.__ifloordiv__,
            [int, float, np.float64, Decimal],
        )

    def __neg__(self) -> Union[int, float, np.float64, Decimal]:
        return self.unary_operation(operator.__neg__, [int, float, np.float64, Decimal])

    def __and__(self, bool_or_bool_indicator) -> bool:
        return self.binary_operation(bool_or_bool_indicator, operator.__and__, [bool])

    def __iand__(self, bool_or_bool_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            bool_or_bool_indicator, operator.__iand__, [bool]
        )

    def __or__(self, bool_or_bool_indicator) -> bool:
        return self.binary_operation(bool_or_bool_indicator, operator.__or__, [bool])

    def __ior__(self, bool_or_bool_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            bool_or_bool_indicator, operator.__ior__, [bool]
        )

    def __bool__(self) -> bool:
        return self.data

    def lt(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__lt__, [int, float, np.float64, Decimal]
        )

    def le(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__le__, [int, float, np.float64, Decimal]
        )

    def eq(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__eq__, [int, float, np.float64, Decimal]
        )

    def ne(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__ne__, [int, float, np.float64, Decimal]
        )

    def ge(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__ge__, [int, float, np.float64, Decimal]
        )

    def gt(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__gt__, [int, float, np.float64, Decimal]
        )

    def add(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__add__, [int, float, np.float64, Decimal]
        )

    def sub(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__sub__, [int, float, np.float64, Decimal]
        )

    def mul(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__mul__, [int, float, np.float64, Decimal]
        )

    def truediv(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator,
            operator.__truediv__,
            [int, float, np.float64, Decimal],
        )

    def floordiv(self, decimal_or_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            decimal_or_indicator,
            operator.__floordiv__,
            [int, float, np.float64, Decimal],
        )

    def neg(self) -> "Indicator":
        return self.indicator_unary_operation(
            operator.__neg__, [int, float, np.float64, Decimal]
        )

    def sand(self, bool_or_bool_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            bool_or_bool_indicator, operator.__and__, [bool]
        )

    def sor(self, bool_or_bool_indicator) -> "Indicator":
        return self.indicator_binary_operation(
            bool_or_bool_indicator, operator.__or__, [bool]
        )


class ZipIndicator(Indicator):
    instances = 0
    count = 0

    def __init__(
        self,
        source_indicator1: Indicator,
        source_indicator2: Indicator,
        operation_function: Callable,
        transform: Callable = lambda new_data: new_data,
    ):
        ZipIndicator.instances += 1
        super().__init__(transform=transform)
        self.source_indicator1 = source_indicator1
        self.source_indicator2 = source_indicator2
        self.data1_queue = deque()
        self.data2_queue = deque()
        self.operation_function = operation_function
        self.count = 0
        self.data = None
        self.callbacks = deque()
        self.source_indicator1.subscribe(
            lambda new_data: self._handle_new_source1_data(new_data)
        )
        self.source_indicator2.subscribe(
            lambda new_data: self._handle_new_source2_data(new_data)
        )

    def handle_new_data(self) -> None:
        ZipIndicator.count += 1
        data1 = self.data1_queue.popleft()
        data2 = self.data2_queue.popleft()
        if data1 is not None and data2 is not None:
            self.data = self.operation_function(data1, data2)
        else:
            self.data = None
        super().handle_new_data(self.data)

    def _handle_new_source1_data(self, new_data: Any) -> None:
        self.data1_queue.append(new_data)
        if self.count < 0:
            self.handle_new_data()
        self.count += 1

    def _handle_new_source2_data(self, new_data: Any) -> None:
        self.data2_queue.append(new_data)
        if self.count > 0:
            self.handle_new_data()
        self.count -= 1
