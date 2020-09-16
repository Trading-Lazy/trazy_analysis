import operator
from decimal import Decimal
from typing import Any, Callable, List

import rx
from rx import Observable, operators
from rx.disposable import Disposable
from rx.subject import Subject


def check_data_type(data, allowed_types: List[type]):
    if data is None:
        return
    data_type = type(data)
    if data_type not in allowed_types:
        raise Exception(
            "data type should be one of {} not {}".format(allowed_types, data_type)
        )


class StreamData(Subject):
    def __init__(
        self,
        source_data: Observable = None,
        transform: Callable = lambda new_data: new_data,
    ):
        super().__init__()
        self.transform = transform
        self.disposable: Disposable = None
        self.source_data = source_data
        self.data = None
        self.observe(self.source_data)

    def _handle_new_data(self, new_data: Any) -> None:
        transformed_data = self.transform(new_data)
        self.data = transformed_data
        self.on_next(transformed_data)

    def observe(self, observable: Observable):
        self.ignore()
        self.source_data = observable
        if self.source_data is not None:
            self.disposable = observable.subscribe(
                lambda new_elt: self._handle_new_data(new_elt)
            )
            if isinstance(observable, StreamData) and self.source_data.data is not None:
                self.data = self.transform(self.source_data.data)

    def ignore(self):
        if self.disposable is not None:
            self.disposable.dispose()
            self.disposable = None

    def unary_operation(
        self, operation_function: Callable[[Any], Any], allowed_types: List[type],
    ) -> "StreamData":
        check_data_type(self.data, allowed_types)
        stream_data: StreamData = StreamData(
            source_data=self, transform=operation_function
        )
        return stream_data

    def binary_operation_stream(
        self, other: "StreamData", operation_function: Callable[[Any, Any], Any]
    ) -> "StreamData":
        combined_observable = rx.zip(self, other).pipe(
            operators.map(
                lambda new_data: operation_function(new_data[0], new_data[1])
                if new_data[0] is not None and new_data[1] is not None
                else None
            )
        )
        stream_data: StreamData = StreamData(source_data=combined_observable)
        return stream_data

    def binary_operation_data(
        self, other, operation_function: Callable[[Any, Any], Any]
    ) -> "StreamData":
        transform = lambda new_data: (operation_function(new_data, other))
        stream_data: StreamData = StreamData(source_data=self, transform=transform)
        return stream_data

    def binary_operation(
        self,
        other,
        operation_function: Callable[[Any, Any], Any],
        allowed_types: List[type],
    ) -> "StreamData":
        check_data_type(self.data, allowed_types)

        other_is_stream = isinstance(other, StreamData)
        other_data = other.data if other_is_stream else other
        check_data_type(other_data, allowed_types)

        if other_is_stream:
            return self.binary_operation_stream(other, operation_function)
        else:
            return self.binary_operation_data(other, operation_function)

    def __lt__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__lt__, [int, float, Decimal]
        )

    def __le__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__le__, [int, float, Decimal]
        )

    def __eq__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__eq__, [int, float, Decimal]
        )

    def __ne__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__ne__, [int, float, Decimal]
        )

    def __ge__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__ge__, [int, float, Decimal]
        )

    def __gt__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__gt__, [int, float, Decimal]
        )

    def __add__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__add__, [int, float, Decimal]
        )

    def __iadd__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__iadd__, [int, float, Decimal]
        )

    def __sub__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__sub__, [int, float, Decimal]
        )

    def __isub__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__isub__, [int, float, Decimal]
        )

    def __mul__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__mul__, [int, float, Decimal]
        )

    def __imul__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__imul__, [int, float, Decimal]
        )

    def __truediv__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__truediv__, [int, float, Decimal]
        )

    def __itruediv__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__itruediv__, [int, float, Decimal]
        )

    def __floordiv__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__floordiv__, [int, float, Decimal]
        )

    def __ifloordiv__(self, decimal_or_stream):
        return self.binary_operation(
            decimal_or_stream, operator.__ifloordiv__, [int, float, Decimal]
        )

    def __neg__(self):
        return self.unary_operation(operator.__neg__, [int, float, Decimal])

    def __and__(self, bool_or_bool_stream):
        return self.binary_operation(bool_or_bool_stream, operator.__and__, [bool])

    def __iand__(self, bool_or_bool_stream):
        return self.binary_operation(bool_or_bool_stream, operator.__iand__, [bool])

    def __or__(self, bool_or_bool_stream):
        return self.binary_operation(bool_or_bool_stream, operator.__or__, [bool])

    def __ior__(self, bool_or_bool_stream):
        return self.binary_operation(bool_or_bool_stream, operator.__ior__, [bool])

    def __bool__(self):
        return self.data
