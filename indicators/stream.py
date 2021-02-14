import operator
from collections import deque
from decimal import Decimal
from typing import Any, Callable, List, Union

from common.helper import check_type


class StreamData:
    instances = 0
    count = 0

    def __init__(
        self,
        source_data: "StreamData" = None,
        transform: Callable = lambda new_data: new_data,
    ):
        StreamData.instances += 1
        self.transform = transform
        self.source_data = source_data
        self.data = None
        self.callback = lambda new_elt: self.handle_new_data(new_elt)
        self.callbacks = deque()
        self.observe(self.source_data)

    def subscribe(self, callback):
        self.callbacks.append(callback)

    def on_next(self, value: Any):
        for callback in self.callbacks:
            callback(value)

    def handle_new_data(self, new_data: Any) -> None:
        StreamData.count += 1
        transformed_data = self.transform(new_data)
        self.data = transformed_data
        self.on_next(transformed_data)

    def observe(self, stream_data: "StreamData"):
        self.ignore()
        self.source_data = stream_data
        if self.source_data is not None:
            stream_data.subscribe(self.callback)
            if (
                isinstance(stream_data, StreamData)
                and self.source_data.data is not None
            ):
                self.data = self.transform(self.source_data.data)

    def remove_callback(self, callback):
        try:
            self.callbacks.remove(callback)
        except ValueError:
            pass

    def ignore(self):
        if self.source_data is not None:
            self.source_data.remove_callback(self.callback)

    def stream_unary_operation(
        self,
        operation_function: Callable[[Any], Any],
        allowed_types: List[type],
    ) -> "StreamData":
        check_type(self.data, allowed_types)
        stream_data: StreamData = StreamData(
            source_data=self, transform=operation_function
        )
        return stream_data

    def stream_binary_operation_stream(
        self, other: "StreamData", operation_function: Callable[[Any, Any], Any]
    ) -> "StreamData":
        stream_zip_data = ZipStreamData(
            source_data1=self, source_data2=other, operation_function=operation_function
        )
        return stream_zip_data

    def stream_binary_operation_data(
        self, other, operation_function: Callable[[Any, Any], Any]
    ) -> "StreamData":
        transform = lambda new_data: (operation_function(new_data, other))
        stream_data: StreamData = StreamData(source_data=self, transform=transform)
        return stream_data

    def stream_binary_operation(
        self,
        other,
        operation_function: Callable[[Any, Any], Any],
        allowed_types: List[type],
    ) -> "StreamData":
        check_type(self.data, allowed_types)

        other_is_stream = isinstance(other, StreamData)
        other_data = other.data if other_is_stream else other
        check_type(other_data, allowed_types)

        if other_is_stream:
            return self.stream_binary_operation_stream(other, operation_function)
        else:
            return self.stream_binary_operation_data(other, operation_function)

    def unary_operation(
        self,
        operation_function: Callable[[Any], Any],
        allowed_types: List[type],
    ) -> "StreamData":
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
        other_is_stream = isinstance(other, StreamData)
        other_data = other.data if other_is_stream else other
        if other_data is None:
            return None
        check_type(other_data, allowed_types)

        if other_is_stream:
            return operation_function(self.data, other.data)
        else:
            return operation_function(self.data, other)

    def __lt__(self, decimal_or_stream) -> bool:
        return self.binary_operation(
            decimal_or_stream, operator.__lt__, [int, float, Decimal]
        )

    def __le__(self, decimal_or_stream) -> bool:
        return self.binary_operation(
            decimal_or_stream, operator.__le__, [int, float, Decimal]
        )

    def __eq__(self, decimal_or_stream) -> bool:
        return self.binary_operation(
            decimal_or_stream, operator.__eq__, [int, float, Decimal]
        )

    def __ne__(self, decimal_or_stream) -> bool:
        return self.binary_operation(
            decimal_or_stream, operator.__ne__, [int, float, Decimal]
        )

    def __ge__(self, decimal_or_stream) -> bool:
        return self.binary_operation(
            decimal_or_stream, operator.__ge__, [int, float, Decimal]
        )

    def __gt__(self, decimal_or_stream) -> bool:
        return self.binary_operation(
            decimal_or_stream, operator.__gt__, [int, float, Decimal]
        )

    def __add__(self, decimal_or_stream) -> Union[int, float, Decimal]:
        return self.binary_operation(
            decimal_or_stream, operator.__add__, [int, float, Decimal]
        )

    def __iadd__(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__iadd__, [int, float, Decimal]
        )

    def __sub__(self, decimal_or_stream) -> Union[int, float, Decimal]:
        return self.binary_operation(
            decimal_or_stream, operator.__sub__, [int, float, Decimal]
        )

    def __isub__(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__isub__, [int, float, Decimal]
        )

    def __mul__(self, decimal_or_stream) -> Union[int, float, Decimal]:
        return self.binary_operation(
            decimal_or_stream, operator.__mul__, [int, float, Decimal]
        )

    def __imul__(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__imul__, [int, float, Decimal]
        )

    def __truediv__(self, decimal_or_stream) -> Union[int, float, Decimal]:
        return self.binary_operation(
            decimal_or_stream, operator.__truediv__, [int, float, Decimal]
        )

    def __itruediv__(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__itruediv__, [int, float, Decimal]
        )

    def __floordiv__(self, decimal_or_stream) -> Union[int, float, Decimal]:
        return self.binary_operation(
            decimal_or_stream, operator.__floordiv__, [int, float, Decimal]
        )

    def __ifloordiv__(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__ifloordiv__, [int, float, Decimal]
        )

    def __neg__(self) -> Union[int, float, Decimal]:
        return self.unary_operation(operator.__neg__, [int, float, Decimal])

    def __and__(self, bool_or_bool_stream) -> bool:
        return self.binary_operation(bool_or_bool_stream, operator.__and__, [bool])

    def __iand__(self, bool_or_bool_stream) -> "StreamData":
        return self.stream_binary_operation(
            bool_or_bool_stream, operator.__iand__, [bool]
        )

    def __or__(self, bool_or_bool_stream) -> bool:
        return self.binary_operation(bool_or_bool_stream, operator.__or__, [bool])

    def __ior__(self, bool_or_bool_stream) -> "StreamData":
        return self.stream_binary_operation(
            bool_or_bool_stream, operator.__ior__, [bool]
        )

    def __bool__(self) -> bool:
        return self.data

    def lt(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__lt__, [int, float, Decimal]
        )

    def le(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__le__, [int, float, Decimal]
        )

    def eq(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__eq__, [int, float, Decimal]
        )

    def ne(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__ne__, [int, float, Decimal]
        )

    def ge(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__ge__, [int, float, Decimal]
        )

    def gt(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__gt__, [int, float, Decimal]
        )

    def add(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__add__, [int, float, Decimal]
        )

    def sub(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__sub__, [int, float, Decimal]
        )

    def mul(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__mul__, [int, float, Decimal]
        )

    def truediv(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__truediv__, [int, float, Decimal]
        )

    def floordiv(self, decimal_or_stream) -> "StreamData":
        return self.stream_binary_operation(
            decimal_or_stream, operator.__floordiv__, [int, float, Decimal]
        )

    def neg(self) -> "StreamData":
        return self.stream_unary_operation(operator.__neg__, [int, float, Decimal])

    def sand(self, bool_or_bool_stream) -> "StreamData":
        return self.stream_binary_operation(
            bool_or_bool_stream, operator.__and__, [bool]
        )

    def sor(self, bool_or_bool_stream) -> "StreamData":
        return self.stream_binary_operation(
            bool_or_bool_stream, operator.__or__, [bool]
        )


class ZipStreamData(StreamData):
    instances = 0
    count = 0

    def __init__(
        self,
        source_data1: StreamData,
        source_data2: StreamData,
        operation_function: Callable,
        transform: Callable = lambda new_data: new_data,
    ):
        ZipStreamData.instances += 1
        super().__init__(transform=transform)
        self.source_data1 = source_data1
        self.source_data2 = source_data2
        self.data1_queue = deque()
        self.data2_queue = deque()
        self.operation_function = operation_function
        self.count = 0
        self.data = None
        self.callbacks = deque()
        self.source_data1.subscribe(
            lambda new_data: self._handle_new_source1_data(new_data)
        )
        self.source_data2.subscribe(
            lambda new_data: self._handle_new_source2_data(new_data)
        )

    def handle_new_data(self) -> None:
        ZipStreamData.count += 1
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
