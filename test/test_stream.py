import operator
from collections import Callable
from decimal import Decimal
from unittest.mock import call, patch

import pytest
import rx
from rx import Observable
from rx.subject import Subject

from indicators.crossover import Crossover
from indicators.stream import StreamData, check_data_type
from test.tools.tools import not_raises


@pytest.mark.parametrize(
    "data, allowed_types, raise_exception",
    [
        (None, [int, float, bool], False),
        (5, [int, float, bool], False),
        (Decimal("2"), [int, float, bool], True),
    ],
)
def test_check_data_type(data, allowed_types, raise_exception):
    if raise_exception:
        with pytest.raises(Exception):
            check_data_type(data, allowed_types)
    else:
        with not_raises(Exception):
            check_data_type(data, allowed_types)


def test_stream_handle_new_data_default_transform():
    source_data = Subject()
    stream_data = StreamData(source_data=source_data)

    new_data = 3
    source_data.on_next(new_data)

    assert stream_data.data == new_data


def test_stream_handle_new_data_custom_transform():
    source_data = Subject()
    stream_data = StreamData(source_data=source_data, transform=lambda x: x * 2)

    source_data.on_next(5)

    assert stream_data.data == 10


@patch("rx.disposable.Disposable.dispose")
def test_ignore_disposable_is_not_none(dispose_mocked):
    source_stream_data = StreamData()
    stream_data = StreamData(source_data=source_stream_data)
    stream_data.ignore()
    dispose_calls = [call()]
    dispose_mocked.assert_has_calls(dispose_calls)


@patch("rx.disposable.Disposable.dispose")
def test_ignore_disposable_is_none(dispose_mocked):
    stream_data = StreamData()
    stream_data.ignore()
    dispose_calls = []
    dispose_mocked.assert_has_calls(dispose_calls)


@patch("indicators.stream.StreamData.ignore")
@patch("rx.subject.Subject.subscribe")
def test_observe_source_data_is_none(subscribe_mocked, ignore_mocked):
    source_data = StreamData()
    stream_data = StreamData(source_data=source_data)
    stream_data.observe(None)

    assert ignore_mocked.call_count == 3
    ignore_calls = [call(), call(), call()]
    ignore_mocked.assert_has_calls(ignore_calls)
    assert subscribe_mocked.call_count == 1

    assert stream_data.source_data is None
    assert stream_data.data is None


@patch("indicators.stream.StreamData.ignore")
@patch("rx.subject.Subject.subscribe")
def test_observe_source_data_is_stream_data_and_has_no_data(
    subscribe_mocked, ignore_mocked
):
    stream_data = StreamData()

    source_data = StreamData()
    stream_data.observe(source_data)

    assert id(stream_data.source_data) == id(source_data)

    assert ignore_mocked.call_count == 3
    ignore_calls = [call(), call(), call()]
    ignore_mocked.assert_has_calls(ignore_calls)

    assert subscribe_mocked.call_count == 1
    for call_args in subscribe_mocked.call_args_list:
        args, kwargs = call_args
        assert isinstance(args[0], Callable)

    assert stream_data.data is None


@patch("indicators.stream.StreamData.ignore")
@patch("rx.subject.Subject.subscribe")
def test_observe_source_data_is_stream_data_and_has_data(
    subscribe_mocked, ignore_mocked
):
    stream_data = StreamData(transform=lambda x: x * 2)

    source_data = StreamData()
    source_data.data = 5
    source_data.on_next(5)
    stream_data.observe(source_data)

    assert id(stream_data.source_data) == id(source_data)

    assert ignore_mocked.call_count == 3
    ignore_calls = [call(), call(), call()]
    ignore_mocked.assert_has_calls(ignore_calls)

    assert subscribe_mocked.call_count == 1
    for call_args in subscribe_mocked.call_args_list:
        args, kwargs = call_args
        assert isinstance(args[0], Callable)

    assert stream_data.data == source_data.data * 2


@patch("indicators.stream.StreamData.ignore")
@patch("rx.core.observable.Observable.subscribe")
def test_observe_source_data_is_observable_and_has_no_data(
    subscribe_mocked, ignore_mocked
):
    stream_data = StreamData()

    source_data = Observable()
    stream_data.observe(source_data)

    assert id(stream_data.source_data) == id(source_data)

    assert ignore_mocked.call_count == 2
    ignore_calls = [call(), call()]
    ignore_mocked.assert_has_calls(ignore_calls)

    assert subscribe_mocked.call_count == 1
    for call_args in subscribe_mocked.call_args_list:
        args, kwargs = call_args
        assert isinstance(args[0], Callable)

    assert stream_data.data is None


@patch("indicators.stream.StreamData.ignore")
@patch("rx.core.observable.Observable.subscribe")
def test_observe_source_data_is_observable_and_has_data(
    subscribe_mocked, ignore_mocked
):
    stream_data = StreamData(transform=lambda x: x * 2)

    source_data = Observable
    stream_data.observe(source_data)

    assert id(stream_data.source_data) == id(source_data)

    assert ignore_mocked.call_count == 2
    ignore_calls = [call(), call()]
    ignore_mocked.assert_has_calls(ignore_calls)

    assert subscribe_mocked.call_count == 1
    for call_args in subscribe_mocked.call_args_list:
        args, kwargs = call_args
        assert isinstance(args[0], Callable)


def test_observe_source_data_is_stream_data_propagation():
    source_data = Subject()
    stream_data = StreamData(source_data=source_data, transform=lambda x: x * 2)

    source_data = StreamData()
    stream_data.observe(source_data)

    source_data.on_next(3)
    assert stream_data.data == 6

    source_data.on_next(-5)
    assert stream_data.data == -10


def test_observe_source_data_is_observable_data_propagation():
    stream_data = StreamData(transform=lambda x: x * 2)

    source_data = rx.of(-6)
    stream_data.observe(source_data)

    assert stream_data.data == -12


def test_unary_operation_data_type_not_in_allowed_types():
    source_data = Subject()
    stream_data = StreamData(source_data=source_data)
    source_data.on_next(Decimal("2"))
    with pytest.raises(Exception):
        stream_data.unary_operation(
            operator_function=operator.__neg__, allowed_types=[int, float]
        )


@patch("rx.subject.Subject.subscribe")
def test_unary_operation_data_is_none(subscribe_mocked):
    stream_data = StreamData()
    derived_stream_data = stream_data.unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert subscribe_mocked.call_count == 1
    assert id(derived_stream_data.source_data) == id(stream_data)
    assert derived_stream_data.data is None
    for call_args in subscribe_mocked.call_args_list:
        args, kwargs = call_args
        assert isinstance(args[0], Callable)


def test_unary_operation_data_is_not_none_and_allowed():
    source_data = Subject()
    stream_data = StreamData(source_data=source_data)
    source_data.on_next(2)
    derived_stream_data = stream_data.unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert id(derived_stream_data.source_data) == id(stream_data)
    assert derived_stream_data.data == -2


def test_unary_operation_propagation():
    source_data = Subject()
    stream_data = StreamData(source_data=source_data)
    source_data.on_next(2)
    derived_stream_data = stream_data.unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert id(derived_stream_data.source_data) == id(stream_data)
    assert derived_stream_data.data == -2

    source_data.on_next(-5)
    assert derived_stream_data.data == 5


def test_binary_operation_stream_data_type_not_in_allowed_types():
    source_data = Subject()
    stream_data = StreamData(source_data=source_data)
    source_data.on_next(Decimal("2"))
    with pytest.raises(Exception):
        stream_data.binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_binary_operation_other_data_type_not_in_allowed_types():
    source_data = Subject()
    stream_data = StreamData(source_data=source_data)
    source_data.on_next(2)

    with pytest.raises(Exception):
        stream_data.binary_operation(
            Decimal("2"), operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_binary_operation_other_stream_data_type_not_in_allowed_types():
    source_data = Subject()
    stream_data = StreamData(source_data)
    source_data.on_next(2)

    other_source_data = Subject()
    other_stream_data = StreamData(source_data=other_source_data)
    other_source_data.on_next(Decimal("5"))

    with pytest.raises(Exception):
        stream_data.binary_operation(
            other_stream_data,
            operator_function=operator.__add__,
            allowed_types=[int, float],
        )


def test_binary_operation_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.binary_operation(
        other_stream, operator.__add__, allowed_types=[int, float]
    )

    assert derived_stream_data.data is None

    # the derived stream is a "zip" meaning it pairs values 2 by 2
    stream_data.on_next(5)
    assert derived_stream_data.data is None

    other_stream.on_next(7)
    assert derived_stream_data.data == 12


def test_binary_operation_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.binary_operation(
        6, operator.__add__, allowed_types=[int, float]
    )

    assert derived_stream_data.data is None

    stream_data.on_next(5)
    assert derived_stream_data.data == 11


def test_lt_data():
    stream_data = StreamData()
    derived_stream_data = stream_data < 5

    stream_data.on_next(2)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    assert derived_stream_data.data == False

    stream_data.on_next(7)
    assert derived_stream_data.data == False


def test_lt_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data < other_stream

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == False

    stream_data.on_next(8)
    other_stream.on_next(6)
    assert derived_stream_data.data == False


def test_le_data():
    stream_data = StreamData()
    derived_stream_data = stream_data <= 5

    stream_data.on_next(2)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    assert derived_stream_data.data == True

    stream_data.on_next(7)
    assert derived_stream_data.data == False


def test_le_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data <= other_stream

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == True

    stream_data.on_next(8)
    other_stream.on_next(6)
    assert derived_stream_data.data == False


def test_eq_data():
    stream_data = StreamData()
    derived_stream_data = stream_data == 5

    stream_data.on_next(2)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    assert derived_stream_data.data == True


def test_eq_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data == other_stream

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == True


def test_ne_data():
    stream_data = StreamData()
    derived_stream_data = stream_data != 5

    stream_data.on_next(2)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    assert derived_stream_data.data == False


def test_ne_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data != other_stream

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == False


def test_ge_data():
    stream_data = StreamData()
    derived_stream_data = stream_data >= 5

    stream_data.on_next(2)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    assert derived_stream_data.data == True

    stream_data.on_next(7)
    assert derived_stream_data.data == True


def test_ge_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data >= other_stream

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == True

    stream_data.on_next(8)
    other_stream.on_next(6)
    assert derived_stream_data.data == True


def test_gt_data():
    stream_data = StreamData()
    derived_stream_data = stream_data > 5

    stream_data.on_next(2)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    assert derived_stream_data.data == False

    stream_data.on_next(7)
    assert derived_stream_data.data == True


def test_gt_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data > other_stream

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == False

    stream_data.on_next(8)
    other_stream.on_next(6)
    assert derived_stream_data.data == True


def test_add_data():
    stream_data = StreamData()
    derived_stream_data = stream_data + 5

    stream_data.on_next(2)
    assert derived_stream_data.data == 7

    stream_data.on_next(5)
    assert derived_stream_data.data == 10


def test_add_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data + other_stream

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == 6

    stream_data.on_next(7)
    other_stream.on_next(2)
    assert derived_stream_data.data == 9


def test_iadd_data():
    stream_data = StreamData()
    initial_stream_data = stream_data
    stream_data += 5

    initial_stream_data.on_next(2)
    assert stream_data.data == 7

    initial_stream_data.on_next(5)
    assert stream_data.data == 10


def test_iadd_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    initial_stream_data = stream_data
    stream_data += other_stream

    initial_stream_data.on_next(2)
    other_stream.on_next(4)
    assert stream_data.data == 6

    initial_stream_data.on_next(7)
    other_stream.on_next(2)
    assert stream_data.data == 9


def test_sub_data():
    stream_data = StreamData()
    derived_stream_data = stream_data - 5

    stream_data.on_next(2)
    assert derived_stream_data.data == -3

    stream_data.on_next(5)
    assert derived_stream_data.data == 0


def test_sub_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data - other_stream

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == -2

    stream_data.on_next(7)
    other_stream.on_next(2)
    assert derived_stream_data.data == 5


def test_special_test():
    stream_data = StreamData()
    other_stream = StreamData()
    crossover = Crossover(stream_data, other_stream)

    stream_data.on_next(2)
    stream_data.on_next(3)
    stream_data.on_next(4)
    other_stream.on_next(5)


def test_isub_data():
    stream_data = StreamData()
    initial_stream_data = stream_data
    stream_data -= 5

    initial_stream_data.on_next(2)
    assert stream_data.data == -3

    initial_stream_data.on_next(5)
    assert stream_data.data == 0


def test_isub_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    initial_stream_data = stream_data
    stream_data -= other_stream

    initial_stream_data.on_next(2)
    other_stream.on_next(4)
    assert stream_data.data == -2

    initial_stream_data.on_next(7)
    other_stream.on_next(2)
    assert stream_data.data == 5


def test_mul_data():
    stream_data = StreamData()
    derived_stream_data = stream_data * 5

    stream_data.on_next(2)
    assert derived_stream_data.data == 10

    stream_data.on_next(-5)
    assert derived_stream_data.data == -25


def test_mul_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data * other_stream

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == 8

    stream_data.on_next(7)
    other_stream.on_next(-2)
    assert derived_stream_data.data == -14


def test_imul_data():
    stream_data = StreamData()
    initial_stream_data = stream_data
    stream_data *= 5

    initial_stream_data.on_next(2)
    assert stream_data.data == 10

    initial_stream_data.on_next(-5)
    assert stream_data.data == -25


def test_imul_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    initial_stream_data = stream_data
    stream_data *= other_stream

    initial_stream_data.on_next(2)
    other_stream.on_next(4)
    assert stream_data.data == 8

    initial_stream_data.on_next(7)
    other_stream.on_next(-2)
    assert stream_data.data == -14


def test_truediv_data():
    stream_data = StreamData()
    derived_stream_data = stream_data / 5

    stream_data.on_next(2)
    assert derived_stream_data.data == 0.4

    stream_data.on_next(-5)
    assert derived_stream_data.data == -1.0


def test_truediv_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data / other_stream

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == 0.5

    stream_data.on_next(7)
    other_stream.on_next(-2)
    assert derived_stream_data.data == -3.5


def test_itruediv_data():
    stream_data = StreamData()
    initial_stream_data = stream_data
    stream_data /= 5

    initial_stream_data.on_next(2)
    assert stream_data.data == 0.4

    initial_stream_data.on_next(-5)
    assert stream_data.data == -1.0


def test_itruediv_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    initial_stream_data = stream_data
    stream_data /= other_stream

    initial_stream_data.on_next(2)
    other_stream.on_next(4)
    assert stream_data.data == 0.5

    initial_stream_data.on_next(7)
    other_stream.on_next(-2)
    assert stream_data.data == -3.5


def test_floordiv_data():
    stream_data = StreamData()
    derived_stream_data = stream_data // 5

    stream_data.on_next(2)
    assert derived_stream_data.data == 0

    stream_data.on_next(16)
    assert derived_stream_data.data == 3

    stream_data.on_next(-13)
    assert derived_stream_data.data == -3


def test_floordiv_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data // other_stream

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == 0

    stream_data.on_next(16)
    other_stream.on_next(5)
    assert derived_stream_data.data == 3

    stream_data.on_next(7)
    other_stream.on_next(-2)
    assert derived_stream_data.data == -4


def test_ifloordiv_data():
    stream_data = StreamData()
    initial_stream_data = stream_data
    stream_data //= 5

    initial_stream_data.on_next(2)
    assert stream_data.data == 0

    initial_stream_data.on_next(16)
    assert stream_data.data == 3

    initial_stream_data.on_next(-14)
    assert stream_data.data == -3


def test_ifloordiv_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    initial_stream_data = stream_data
    stream_data //= other_stream

    initial_stream_data.on_next(2)
    other_stream.on_next(4)
    assert stream_data.data == 0

    initial_stream_data.on_next(16)
    other_stream.on_next(5)
    assert stream_data.data == 3

    initial_stream_data.on_next(7)
    other_stream.on_next(-2)
    assert stream_data.data == -4


def test_neg():
    stream_data = StreamData()
    derived_stream_data = -stream_data

    stream_data.on_next(2)
    assert derived_stream_data.data == -2

    stream_data.on_next(-3)
    assert derived_stream_data.data == 3


def test_and_data():
    stream_data = StreamData()
    derived_stream_data = stream_data & True

    stream_data.on_next(True)
    assert derived_stream_data.data == True

    stream_data.on_next(False)
    assert derived_stream_data.data == False

    derived_stream_data = stream_data & False

    stream_data.on_next(True)
    assert derived_stream_data.data == False

    stream_data.on_next(False)
    assert derived_stream_data.data == False


def test_and_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data & other_stream

    stream_data.on_next(True)
    other_stream.on_next(True)
    assert derived_stream_data.data == True

    stream_data.on_next(True)
    other_stream.on_next(False)
    assert derived_stream_data.data == False

    stream_data.on_next(False)
    other_stream.on_next(True)
    assert derived_stream_data.data == False

    stream_data.on_next(False)
    other_stream.on_next(False)
    assert derived_stream_data.data == False


def test_iand_data():
    stream_data = StreamData()
    initial_stream_data = stream_data
    stream_data &= True

    initial_stream_data.on_next(False)
    assert stream_data.data == False

    initial_stream_data.on_next(True)
    assert stream_data.data == True

    initial_stream_data = stream_data
    stream_data &= False

    initial_stream_data.on_next(False)
    assert stream_data.data == False

    initial_stream_data.on_next(True)
    assert stream_data.data == False


def test_iand_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    initial_stream_data = stream_data
    stream_data &= other_stream

    initial_stream_data.on_next(True)
    other_stream.on_next(True)
    assert stream_data.data == True

    initial_stream_data.on_next(True)
    other_stream.on_next(False)
    assert stream_data.data == False

    initial_stream_data.on_next(False)
    other_stream.on_next(True)
    assert stream_data.data == False

    initial_stream_data.on_next(False)
    other_stream.on_next(False)
    assert stream_data.data == False


def test_or_data():
    stream_data = StreamData()
    derived_stream_data = stream_data | True

    stream_data.on_next(True)
    assert derived_stream_data.data == True

    stream_data.on_next(False)
    assert derived_stream_data.data == True

    derived_stream_data = stream_data | False

    stream_data.on_next(True)
    assert derived_stream_data.data == True

    stream_data.on_next(False)
    assert derived_stream_data.data == False


def test_or_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data | other_stream

    stream_data.on_next(True)
    other_stream.on_next(True)
    assert derived_stream_data.data == True

    stream_data.on_next(True)
    other_stream.on_next(False)
    assert derived_stream_data.data == True

    stream_data.on_next(False)
    other_stream.on_next(True)
    assert derived_stream_data.data == True

    stream_data.on_next(False)
    other_stream.on_next(False)
    assert derived_stream_data.data == False


def test_ior_data():
    stream_data = StreamData()
    initial_stream_data = stream_data
    stream_data |= True

    initial_stream_data.on_next(False)
    assert stream_data.data == True

    initial_stream_data.on_next(True)
    assert stream_data.data == True

    initial_stream_data = stream_data
    stream_data |= False

    initial_stream_data.on_next(False)
    assert stream_data.data == False

    initial_stream_data.on_next(True)
    assert stream_data.data == True


def test_ior_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    initial_stream_data = stream_data
    stream_data |= other_stream

    initial_stream_data.on_next(True)
    other_stream.on_next(True)
    assert stream_data.data == True

    initial_stream_data.on_next(True)
    other_stream.on_next(False)
    assert stream_data.data == True

    initial_stream_data.on_next(False)
    other_stream.on_next(True)
    assert stream_data.data == True

    initial_stream_data.on_next(False)
    other_stream.on_next(False)
    assert stream_data.data == False


def test_bool():
    source_data = Subject()
    stream_data = StreamData(source_data=source_data)

    source_data.on_next(True)
    assert stream_data

    source_data.on_next(False)
    assert not stream_data
