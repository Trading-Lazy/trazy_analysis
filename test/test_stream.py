import operator

import pytest

from indicators.stream import StreamData, ZipStreamData


def test_handle_new_data_default_transform():
    stream_data = StreamData()

    new_data = 3
    stream_data.handle_new_data(new_data)

    assert stream_data.data == new_data


def test_handle_new_data_custom_transform():
    stream_data = StreamData(transform=lambda x: x * 2)

    stream_data.handle_new_data(5)

    assert stream_data.data == 10


def test_ignore_source_data_is_not_none():
    source_stream_data = StreamData()
    stream_data = StreamData(source_data=source_stream_data)
    assert len(source_stream_data.callbacks) == 1
    stream_data.ignore()
    assert len(source_stream_data.callbacks) == 0


def test_ignore_source_data_is_none():
    stream_data = StreamData()
    assert len(stream_data.callbacks) == 0
    stream_data.ignore()
    assert len(stream_data.callbacks) == 0


def test_observe_source_data_is_none():
    source_data = StreamData()
    stream_data = StreamData(source_data=source_data)
    stream_data.observe(None)
    assert stream_data.source_data is None
    assert stream_data.data is None


def test_observe_source_data_is_stream_data_and_has_no_data():
    stream_data = StreamData()

    source_data = StreamData()
    stream_data.observe(source_data)

    assert id(stream_data.source_data) == id(source_data)
    assert stream_data.data is None


def test_observe_source_data_is_stream_data_and_has_data():
    stream_data = StreamData(transform=lambda x: x * 2)

    source_data = StreamData()
    source_data.data = 5
    source_data.on_next(5)
    stream_data.observe(source_data)

    assert id(stream_data.source_data) == id(source_data)
    assert stream_data.data == source_data.data * 2


def test_observe_source_data_is_observable_and_has_no_data():
    stream_data = StreamData()

    source_data = StreamData()
    stream_data.observe(source_data)

    assert id(stream_data.source_data) == id(source_data)

    assert stream_data.data is None


def test_observe_source_data_is_observable_and_has_data():
    stream_data = StreamData(transform=lambda x: x * 2)

    source_data = StreamData()
    source_data.handle_new_data(-6)

    stream_data.observe(source_data)
    assert id(stream_data.source_data) == id(source_data)
    assert stream_data.data == -12


def test_observe_source_data_is_stream_data_propagation():
    stream_data = StreamData(transform=lambda x: x * 2)

    source_data = StreamData()
    stream_data.observe(source_data)

    source_data.on_next(3)
    assert stream_data.data == 6

    source_data.on_next(-5)
    assert stream_data.data == -10


def test_observe_source_data_is_observable_data_propagation():
    stream_data = StreamData(transform=lambda x: x * 2)

    source_data = StreamData()
    source_data.handle_new_data(-6)
    stream_data.observe(source_data)

    assert stream_data.data == -12


def test_unary_operation_data_type_not_in_allowed_types():
    stream_data = StreamData()
    stream_data.handle_new_data(2)
    with pytest.raises(Exception):
        stream_data.unary_operation(
            operator_function=operator.__neg__, allowed_types=[int, float]
        )


def test_unary_operation_data_is_none():
    stream_data = StreamData()
    unary_operation_result = stream_data.unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert unary_operation_result is None


def test_unary_operation_data_is_not_none_and_allowed():
    stream_data = StreamData()
    stream_data.handle_new_data(2)
    unary_operation_result = stream_data.unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert unary_operation_result == -2


def test_binary_operation_stream_data_type_not_in_allowed_types():
    stream_data = StreamData()
    stream_data.handle_new_data(2)
    with pytest.raises(Exception):
        stream_data.binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_binary_operation_other_data_type_not_in_allowed_types():
    stream_data = StreamData()
    stream_data.handle_new_data(2)

    with pytest.raises(Exception):
        stream_data.binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_binary_operation_other_stream_data_type_not_in_allowed_types():
    stream_data = StreamData()
    stream_data.handle_new_data(2)

    other_stream_data = StreamData()
    other_stream_data.handle_new_data(5)

    with pytest.raises(Exception):
        stream_data.binary_operation(
            other_stream_data,
            operator_function=operator.__add__,
            allowed_types=[int, float],
        )


def test_binary_operation_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    binary_operation_result = stream_data.binary_operation(
        other_stream, operator.__add__, allowed_types=[int, float]
    )

    assert binary_operation_result is None

    # the derived stream is a "zip" meaning it pairs values 2 by 2
    stream_data.handle_new_data(5)
    binary_operation_result = stream_data.binary_operation(
        other_stream, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation_result is None

    other_stream.handle_new_data(7)
    binary_operation_result = stream_data.binary_operation(
        other_stream, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation_result == 12


def test_binary_operation_data():
    stream_data = StreamData()
    binary_operation = stream_data.binary_operation(
        6, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation is None

    stream_data.handle_new_data(5)
    binary_operation = stream_data.binary_operation(
        6, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation == 11


def test_lt_data():
    stream_data = StreamData()

    stream_data.handle_new_data(2)
    assert stream_data < 5

    stream_data.handle_new_data(5)
    assert not (stream_data < 5)

    stream_data.handle_new_data(7)
    assert not (stream_data < 5)


def test_lt_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(2)
    other_stream.handle_new_data(4)
    assert stream_data < other_stream

    stream_data.handle_new_data(5)
    other_stream.handle_new_data(5)
    assert not (stream_data < other_stream)

    stream_data.handle_new_data(8)
    other_stream.handle_new_data(6)
    assert not (stream_data < other_stream)


def test_le_data():
    stream_data = StreamData()

    stream_data.handle_new_data(2)
    assert stream_data <= 5

    stream_data.handle_new_data(5)
    assert stream_data <= 5

    stream_data.handle_new_data(7)
    assert not (stream_data <= 5)


def test_le_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(2)
    other_stream.handle_new_data(4)
    assert stream_data <= other_stream

    stream_data.handle_new_data(5)
    other_stream.handle_new_data(5)
    assert stream_data <= other_stream

    stream_data.handle_new_data(8)
    other_stream.handle_new_data(6)
    assert not (stream_data <= other_stream)


def test_eq_data():
    stream_data = StreamData()

    stream_data.handle_new_data(2)
    assert not (stream_data == 5)

    stream_data.handle_new_data(5)
    assert stream_data == 5


def test_eq_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(2)
    other_stream.handle_new_data(4)
    assert not (stream_data == other_stream)

    stream_data.handle_new_data(5)
    other_stream.handle_new_data(5)
    assert stream_data == other_stream


def test_ne_data():
    stream_data = StreamData()

    stream_data.handle_new_data(2)
    assert stream_data != 5

    stream_data.handle_new_data(5)
    assert not (stream_data != 5)


def test_ne_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(2)
    other_stream.handle_new_data(4)
    assert stream_data != other_stream

    stream_data.handle_new_data(5)
    other_stream.handle_new_data(5)
    assert not (stream_data != other_stream)


def test_ge_data():
    stream_data = StreamData()

    stream_data.handle_new_data(2)
    assert not (stream_data >= 5)

    stream_data.handle_new_data(5)
    assert stream_data >= 5

    stream_data.handle_new_data(7)
    assert stream_data >= 5


def test_ge_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(2)
    other_stream.handle_new_data(4)
    assert not (stream_data >= other_stream)

    stream_data.handle_new_data(5)
    other_stream.handle_new_data(5)
    assert stream_data >= other_stream

    stream_data.handle_new_data(8)
    other_stream.handle_new_data(6)
    assert stream_data >= other_stream


def test_gt_data():
    stream_data = StreamData()

    stream_data.handle_new_data(2)
    assert not (stream_data > 5)

    stream_data.handle_new_data(5)
    assert not (stream_data > 5)

    stream_data.handle_new_data(7)
    assert stream_data > 5


def test_gt_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(2)
    other_stream.handle_new_data(4)
    assert not (stream_data > other_stream)

    stream_data.handle_new_data(5)
    other_stream.handle_new_data(5)
    assert not (stream_data > other_stream)

    stream_data.handle_new_data(8)
    other_stream.handle_new_data(6)
    assert stream_data > other_stream


def test_add_data():
    stream_data = StreamData()

    stream_data.handle_new_data(2)
    assert (stream_data + 5) == 7

    stream_data.handle_new_data(5)
    assert (stream_data + 5) == 10


def test_add_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(2)
    other_stream.handle_new_data(4)
    assert (stream_data + other_stream) == 6

    stream_data.handle_new_data(7)
    other_stream.handle_new_data(2)
    assert (stream_data + other_stream) == 9


def test_sub_data():
    stream_data = StreamData()
    derived_stream_data = stream_data - 5

    stream_data.handle_new_data(2)
    assert (stream_data - 5) == -3

    stream_data.handle_new_data(5)
    assert (stream_data - 5) == 0


def test_sub_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data - other_stream

    stream_data.handle_new_data(2)
    other_stream.handle_new_data(4)
    assert (stream_data - other_stream) == -2

    stream_data.handle_new_data(7)
    other_stream.handle_new_data(2)
    assert (stream_data - other_stream) == 5


def test_mul_data():
    stream_data = StreamData()
    derived_stream_data = stream_data * 5

    stream_data.handle_new_data(2)
    assert (stream_data * 5) == 10

    stream_data.handle_new_data(-5)
    assert (stream_data * 5) == -25


def test_mul_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(2)
    other_stream.handle_new_data(4)
    assert (stream_data * other_stream) == 8

    stream_data.handle_new_data(7)
    other_stream.handle_new_data(-2)
    assert (stream_data * other_stream) == -14


def test_truediv_data():
    stream_data = StreamData()

    stream_data.handle_new_data(2)
    assert (stream_data / 5) == 0.4

    stream_data.handle_new_data(-5)
    assert (stream_data / 5) == -1.0


def test_truediv_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(2)
    other_stream.handle_new_data(4)
    assert (stream_data / other_stream) == 0.5

    stream_data.handle_new_data(7)
    other_stream.handle_new_data(-2)
    assert (stream_data / other_stream) == -3.5


def test_floordiv_data():
    stream_data = StreamData()

    stream_data.handle_new_data(2)
    assert (stream_data // 5) == 0

    stream_data.handle_new_data(16)
    assert (stream_data // 5) == 3

    stream_data.handle_new_data(-13)
    assert (stream_data // 5) == -3


def test_floordiv_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(2)
    other_stream.handle_new_data(4)
    assert (stream_data // other_stream) == 0

    stream_data.handle_new_data(16)
    other_stream.handle_new_data(5)
    assert (stream_data // other_stream) == 3

    stream_data.handle_new_data(7)
    other_stream.handle_new_data(-2)
    assert (stream_data // other_stream) == -4


def test_neg():
    stream_data = StreamData()

    stream_data.handle_new_data(2)
    assert -stream_data == -2

    stream_data.handle_new_data(-3)
    assert -stream_data == 3


def test_and_data():
    stream_data = StreamData()

    stream_data.handle_new_data(True)
    assert (stream_data & True) == True

    stream_data.handle_new_data(False)
    assert (stream_data & True) == False

    derived_stream_data = stream_data.sand(False)

    stream_data.handle_new_data(True)
    assert (stream_data & False) == False

    stream_data.handle_new_data(False)
    assert (stream_data & False) == False


def test_and_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(True)
    other_stream.handle_new_data(True)
    assert (stream_data & other_stream) == True

    stream_data.handle_new_data(True)
    other_stream.handle_new_data(False)
    assert (stream_data & other_stream) == False

    stream_data.handle_new_data(False)
    other_stream.handle_new_data(True)
    assert (stream_data & other_stream) == False

    stream_data.handle_new_data(False)
    other_stream.handle_new_data(False)
    assert (stream_data & other_stream) == False


def test_or_data():
    stream_data = StreamData()

    stream_data.handle_new_data(True)
    assert (stream_data | True) == True

    stream_data.handle_new_data(False)
    assert (stream_data | True) == True

    stream_data.handle_new_data(True)
    assert (stream_data | False) == True

    stream_data.handle_new_data(False)
    assert (stream_data | False) == False


def test_or_stream():
    stream_data = StreamData()
    other_stream = StreamData()

    stream_data.handle_new_data(True)
    other_stream.handle_new_data(True)
    assert (stream_data | other_stream) == True

    stream_data.handle_new_data(True)
    other_stream.handle_new_data(False)
    assert (stream_data | other_stream) == True

    stream_data.handle_new_data(False)
    other_stream.handle_new_data(True)
    assert (stream_data | other_stream) == True

    stream_data.handle_new_data(False)
    other_stream.handle_new_data(False)
    assert (stream_data | other_stream) == False


def test_bool():
    stream_data = StreamData()

    stream_data.handle_new_data(True)
    assert stream_data

    stream_data.handle_new_data(False)
    assert not stream_data


def test_stream_unary_operation_data_type_not_in_allowed_types():
    stream_data = StreamData()
    stream_data.handle_new_data(2)
    with pytest.raises(Exception):
        stream_data.stream_unary_operation(
            operator_function=operator.__neg__, allowed_types=[int, float]
        )


def test_stream_unary_operation_data_is_none():
    stream_data = StreamData()
    derived_stream_data = stream_data.stream_unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert id(derived_stream_data.source_data) == id(stream_data)
    assert derived_stream_data.data is None


def test_stream_unary_operation_data_is_not_none_and_allowed():
    stream_data = StreamData()
    stream_data.handle_new_data(2)
    derived_stream_data = stream_data.stream_unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert id(derived_stream_data.source_data) == id(stream_data)
    assert derived_stream_data.data == -2


def test_stream_unary_operation_propagation():
    stream_data = StreamData()
    stream_data.handle_new_data(2)
    derived_stream_data = stream_data.stream_unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert id(derived_stream_data.source_data) == id(stream_data)
    assert derived_stream_data.data == -2

    stream_data.handle_new_data(-5)
    assert derived_stream_data.data == 5


def test_stream_binary_operation_stream_data_type_not_in_allowed_types():
    stream_data = StreamData()
    stream_data.handle_new_data(2)
    with pytest.raises(Exception):
        stream_data.stream_binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_stream_binary_operation_other_data_type_not_in_allowed_types():
    stream_data = StreamData()
    stream_data.handle_new_data(2)

    with pytest.raises(Exception):
        stream_data.stream_binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_stream_binary_operation_other_stream_data_type_not_in_allowed_types():
    stream_data = StreamData()
    stream_data.handle_new_data(2)

    other_stream_data = StreamData()
    other_stream_data.handle_new_data(5)

    with pytest.raises(Exception):
        stream_data.stream_binary_operation(
            other_stream_data,
            operator_function=operator.__add__,
            allowed_types=[int, float],
        )


def test_stream_binary_operation_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.stream_binary_operation(
        other_stream, operator.__add__, allowed_types=[int, float]
    )

    assert derived_stream_data.data is None

    # the derived stream is a "zip" meaning it pairs values 2 by 2
    stream_data.on_next(5)
    assert derived_stream_data.data is None

    other_stream.on_next(7)
    assert derived_stream_data.data == 12


def test_stream_binary_operation_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.stream_binary_operation(
        6, operator.__add__, allowed_types=[int, float]
    )

    assert derived_stream_data.data is None

    stream_data.on_next(5)
    assert derived_stream_data.data == 11


def test_stream_lt_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.lt(5)

    stream_data.on_next(2)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    assert derived_stream_data.data == False

    stream_data.on_next(7)
    assert derived_stream_data.data == False


def test_stream_lt_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.lt(other_stream)

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == False

    stream_data.on_next(8)
    other_stream.on_next(6)
    assert derived_stream_data.data == False


def test_stream_le_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.le(5)

    stream_data.on_next(2)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    assert derived_stream_data.data == True

    stream_data.on_next(7)
    assert derived_stream_data.data == False


def test_stream_le_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.le(other_stream)

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == True

    stream_data.on_next(8)
    other_stream.on_next(6)
    assert derived_stream_data.data == False


def test_stream_eq_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.eq(5)

    stream_data.on_next(2)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    assert derived_stream_data.data == True


def test_stream_eq_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.eq(other_stream)

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == True


def test_stream_ne_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.ne(5)

    stream_data.on_next(2)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    assert derived_stream_data.data == False


def test_stream_ne_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.ne(other_stream)

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == True

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == False


def test_stream_ge_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.ge(5)

    stream_data.on_next(2)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    assert derived_stream_data.data == True

    stream_data.on_next(7)
    assert derived_stream_data.data == True


def test_stream_ge_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.ge(other_stream)

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == True

    stream_data.on_next(8)
    other_stream.on_next(6)
    assert derived_stream_data.data == True


def test_stream_gt_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.gt(5)

    stream_data.on_next(2)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    assert derived_stream_data.data == False

    stream_data.on_next(7)
    assert derived_stream_data.data == True


def test_stream_gt_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.gt(other_stream)

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == False

    stream_data.on_next(5)
    other_stream.on_next(5)
    assert derived_stream_data.data == False

    stream_data.on_next(8)
    other_stream.on_next(6)
    assert derived_stream_data.data == True


def test_stream_add_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.add(5)

    stream_data.on_next(2)
    assert derived_stream_data.data == 7

    stream_data.on_next(5)
    assert derived_stream_data.data == 10


def test_stream_add_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.add(other_stream)

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


def test_stream_sub_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.sub(5)

    stream_data.on_next(2)
    assert derived_stream_data.data == -3

    stream_data.on_next(5)
    assert derived_stream_data.data == 0


def test_stream_sub_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.sub(other_stream)

    stream_data.on_next(2)
    other_stream.on_next(4)
    assert derived_stream_data.data == -2

    stream_data.on_next(7)
    other_stream.on_next(2)
    assert derived_stream_data.data == 5


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


def test_stream_mul_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.mul(5)

    stream_data.on_next(2)
    assert derived_stream_data.data == 10

    stream_data.on_next(-5)
    assert derived_stream_data.data == -25


def test_stream_mul_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.mul(other_stream)

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


def test_stream_truediv_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.truediv(5)

    stream_data.on_next(2)
    assert derived_stream_data.data == 0.4

    stream_data.on_next(-5)
    assert derived_stream_data.data == -1.0


def test_stream_truediv_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.truediv(other_stream)

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


def test_stream_floordiv_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.floordiv(5)

    stream_data.on_next(2)
    assert derived_stream_data.data == 0

    stream_data.on_next(16)
    assert derived_stream_data.data == 3

    stream_data.on_next(-13)
    assert derived_stream_data.data == -3


def test_stream_floordiv_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.floordiv(other_stream)

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


def test_stream_neg():
    stream_data = StreamData()
    derived_stream_data = stream_data.neg()

    stream_data.on_next(2)
    assert derived_stream_data.data == -2

    stream_data.on_next(-3)
    assert derived_stream_data.data == 3


def test_stream_and_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.sand(True)

    stream_data.on_next(True)
    assert derived_stream_data.data == True

    stream_data.on_next(False)
    assert derived_stream_data.data == False

    derived_stream_data = stream_data.sand(False)

    stream_data.on_next(True)
    assert derived_stream_data.data == False

    stream_data.on_next(False)
    assert derived_stream_data.data == False


def test_stream_and_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.sand(other_stream)

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


def test_stream_or_data():
    stream_data = StreamData()
    derived_stream_data = stream_data.sor(True)

    stream_data.on_next(True)
    assert derived_stream_data.data == True

    stream_data.on_next(False)
    assert derived_stream_data.data == True

    derived_stream_data = stream_data.sor(False)

    stream_data.on_next(True)
    assert derived_stream_data.data == True

    stream_data.on_next(False)
    assert derived_stream_data.data == False


def test_stream_or_stream():
    stream_data = StreamData()
    other_stream = StreamData()
    derived_stream_data = stream_data.sor(other_stream)

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


def test_zip_stream_data():
    stream_data = StreamData()
    other_stream = StreamData()
    zip_stream_data = ZipStreamData(
        source_data1=stream_data,
        source_data2=other_stream,
        operation_function=operator.__add__,
    )

    other_stream.on_next(None)
    assert zip_stream_data.data is None
    stream_data.on_next(None)
    assert zip_stream_data.data is None

    stream_data.on_next(2)
    assert zip_stream_data.data is None
    stream_data.on_next(3)
    assert zip_stream_data.data is None
    stream_data.on_next(4)
    assert zip_stream_data.data is None

    other_stream.on_next(5)
    assert zip_stream_data.data == 7
    other_stream.on_next(6)
    assert zip_stream_data.data == 9
    other_stream.on_next(7)
    assert zip_stream_data.data == 11

    other_stream.on_next(8)
    assert zip_stream_data.data == 11
