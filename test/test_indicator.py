import operator

import pytest

from trazy_analysis.indicators.indicator import Indicator, ZipIndicator


def test_handle_new_data_default_transform():
    indicator = Indicator()

    new_data = 3
    indicator.handle_new_data(new_data)

    assert indicator.data == new_data


def test_handle_new_data_custom_transform():
    indicator = Indicator(transform=lambda x: x * 2)

    indicator.handle_new_data(5)

    assert indicator.data == 10


def test_ignore_source_indicator_is_not_none():
    source_indicator = Indicator()
    indicator = Indicator(source_indicator=source_indicator)
    assert len(source_indicator.callbacks) == 1
    indicator.ignore()
    assert len(source_indicator.callbacks) == 0


def test_ignore_source_indicator_is_none():
    indicator = Indicator()
    assert len(indicator.callbacks) == 0
    indicator.ignore()
    assert len(indicator.callbacks) == 0


def test_observe_source_indicator_is_none():
    source_indicator = Indicator()
    indicator = Indicator(source_indicator=source_indicator)
    indicator.observe(None)
    assert indicator.source_indicator is None
    assert indicator.data is None


def test_observe_source_indicator_is_indicator_and_has_no_data():
    indicator = Indicator()

    source_indicator = Indicator()
    indicator.observe(source_indicator)

    assert id(indicator.source_indicator) == id(source_indicator)
    assert indicator.data is None


def test_observe_source_indicator_is_indicator_and_has_data():
    indicator = Indicator(transform=lambda x: x * 2)

    source_indicator = Indicator()
    source_indicator.data = 5
    source_indicator.on_next(5)
    indicator.observe(source_indicator)

    assert id(indicator.source_indicator) == id(source_indicator)
    assert indicator.data == source_indicator.data * 2


def test_observe_source_indicator_is_observable_and_has_no_data():
    indicator = Indicator()

    source_indicator = Indicator()
    indicator.observe(source_indicator)

    assert id(indicator.source_indicator) == id(source_indicator)

    assert indicator.data is None


def test_observe_source_indicator_is_observable_and_has_data():
    indicator = Indicator(transform=lambda x: x * 2)

    source_indicator = Indicator()
    source_indicator.handle_new_data(-6)

    indicator.observe(source_indicator)
    assert id(indicator.source_indicator) == id(source_indicator)
    assert indicator.data == -12


def test_observe_source_indicator_is_indicator_propagation():
    indicator = Indicator(transform=lambda x: x * 2)

    source_indicator = Indicator()
    indicator.observe(source_indicator)

    source_indicator.on_next(3)
    assert indicator.data == 6

    source_indicator.on_next(-5)
    assert indicator.data == -10


def test_observe_source_indicator_is_observable_data_propagation():
    indicator = Indicator(transform=lambda x: x * 2)

    source_indicator = Indicator()
    source_indicator.handle_new_data(-6)
    indicator.observe(source_indicator)

    assert indicator.data == -12


def test_unary_operation_data_type_not_in_allowed_types():
    indicator = Indicator()
    indicator.handle_new_data(2)
    with pytest.raises(Exception):
        indicator.unary_operation(
            operator_function=operator.__neg__, allowed_types=[int, float]
        )


def test_unary_operation_data_is_none():
    indicator = Indicator()
    unary_operation_result = indicator.unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert unary_operation_result is None


def test_unary_operation_data_is_not_none_and_allowed():
    indicator = Indicator()
    indicator.handle_new_data(2)
    unary_operation_result = indicator.unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert unary_operation_result == -2


def test_binary_operation_indicator_type_not_in_allowed_types():
    indicator = Indicator()
    indicator.handle_new_data(2)
    with pytest.raises(Exception):
        indicator.binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_binary_operation_other_data_type_not_in_allowed_types():
    indicator = Indicator()
    indicator.handle_new_data(2)

    with pytest.raises(Exception):
        indicator.binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_binary_operation_other_indicator_type_not_in_allowed_types():
    indicator = Indicator()
    indicator.handle_new_data(2)

    other_indicator = Indicator()
    other_indicator.handle_new_data(5)

    with pytest.raises(Exception):
        indicator.binary_operation(
            other_indicator,
            operator_function=operator.__add__,
            allowed_types=[int, float],
        )


def test_binary_operation_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    binary_operation_result = indicator.binary_operation(
        other_indicator, operator.__add__, allowed_types=[int, float]
    )

    assert binary_operation_result is None

    # the derived indicator is a "zip" meaning it pairs values 2 by 2
    indicator.handle_new_data(5)
    binary_operation_result = indicator.binary_operation(
        other_indicator, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation_result is None

    other_indicator.handle_new_data(7)
    binary_operation_result = indicator.binary_operation(
        other_indicator, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation_result == 12


def test_binary_operation_data():
    indicator = Indicator()
    binary_operation = indicator.binary_operation(
        6, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation is None

    indicator.handle_new_data(5)
    binary_operation = indicator.binary_operation(
        6, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation == 11


def test_lt_data():
    indicator = Indicator()

    indicator.handle_new_data(2)
    assert indicator < 5

    indicator.handle_new_data(5)
    assert not (indicator < 5)

    indicator.handle_new_data(7)
    assert not (indicator < 5)


def test_lt_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(2)
    other_indicator.handle_new_data(4)
    assert indicator < other_indicator

    indicator.handle_new_data(5)
    other_indicator.handle_new_data(5)
    assert not (indicator < other_indicator)

    indicator.handle_new_data(8)
    other_indicator.handle_new_data(6)
    assert not (indicator < other_indicator)


def test_le_data():
    indicator = Indicator()

    indicator.handle_new_data(2)
    assert indicator <= 5

    indicator.handle_new_data(5)
    assert indicator <= 5

    indicator.handle_new_data(7)
    assert not (indicator <= 5)


def test_le_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(2)
    other_indicator.handle_new_data(4)
    assert indicator <= other_indicator

    indicator.handle_new_data(5)
    other_indicator.handle_new_data(5)
    assert indicator <= other_indicator

    indicator.handle_new_data(8)
    other_indicator.handle_new_data(6)
    assert not (indicator <= other_indicator)


def test_eq_data():
    indicator = Indicator()

    indicator.handle_new_data(2)
    assert not (indicator == 5)

    indicator.handle_new_data(5)
    assert indicator == 5


def test_eq_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(2)
    other_indicator.handle_new_data(4)
    assert not (indicator == other_indicator)

    indicator.handle_new_data(5)
    other_indicator.handle_new_data(5)
    assert indicator == other_indicator


def test_ne_data():
    indicator = Indicator()

    indicator.handle_new_data(2)
    assert indicator != 5

    indicator.handle_new_data(5)
    assert not (indicator != 5)


def test_ne_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(2)
    other_indicator.handle_new_data(4)
    assert indicator != other_indicator

    indicator.handle_new_data(5)
    other_indicator.handle_new_data(5)
    assert not (indicator != other_indicator)


def test_ge_data():
    indicator = Indicator()

    indicator.handle_new_data(2)
    assert not (indicator >= 5)

    indicator.handle_new_data(5)
    assert indicator >= 5

    indicator.handle_new_data(7)
    assert indicator >= 5


def test_ge_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(2)
    other_indicator.handle_new_data(4)
    assert not (indicator >= other_indicator)

    indicator.handle_new_data(5)
    other_indicator.handle_new_data(5)
    assert indicator >= other_indicator

    indicator.handle_new_data(8)
    other_indicator.handle_new_data(6)
    assert indicator >= other_indicator


def test_gt_data():
    indicator = Indicator()

    indicator.handle_new_data(2)
    assert not (indicator > 5)

    indicator.handle_new_data(5)
    assert not (indicator > 5)

    indicator.handle_new_data(7)
    assert indicator > 5


def test_gt_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(2)
    other_indicator.handle_new_data(4)
    assert not (indicator > other_indicator)

    indicator.handle_new_data(5)
    other_indicator.handle_new_data(5)
    assert not (indicator > other_indicator)

    indicator.handle_new_data(8)
    other_indicator.handle_new_data(6)
    assert indicator > other_indicator


def test_add_data():
    indicator = Indicator()

    indicator.handle_new_data(2)
    assert (indicator + 5) == 7

    indicator.handle_new_data(5)
    assert (indicator + 5) == 10


def test_add_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(2)
    other_indicator.handle_new_data(4)
    assert (indicator + other_indicator) == 6

    indicator.handle_new_data(7)
    other_indicator.handle_new_data(2)
    assert (indicator + other_indicator) == 9


def test_sub_data():
    indicator = Indicator()
    derived_indicator = indicator - 5

    indicator.handle_new_data(2)
    assert (indicator - 5) == -3

    indicator.handle_new_data(5)
    assert (indicator - 5) == 0


def test_sub_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator - other_indicator

    indicator.handle_new_data(2)
    other_indicator.handle_new_data(4)
    assert (indicator - other_indicator) == -2

    indicator.handle_new_data(7)
    other_indicator.handle_new_data(2)
    assert (indicator - other_indicator) == 5


def test_mul_data():
    indicator = Indicator()
    derived_indicator = indicator * 5

    indicator.handle_new_data(2)
    assert (indicator * 5) == 10

    indicator.handle_new_data(-5)
    assert (indicator * 5) == -25


def test_mul_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(2)
    other_indicator.handle_new_data(4)
    assert (indicator * other_indicator) == 8

    indicator.handle_new_data(7)
    other_indicator.handle_new_data(-2)
    assert (indicator * other_indicator) == -14


def test_truediv_data():
    indicator = Indicator()

    indicator.handle_new_data(2)
    assert (indicator / 5) == 0.4

    indicator.handle_new_data(-5)
    assert (indicator / 5) == -1.0


def test_truediv_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(2)
    other_indicator.handle_new_data(4)
    assert (indicator / other_indicator) == 0.5

    indicator.handle_new_data(7)
    other_indicator.handle_new_data(-2)
    assert (indicator / other_indicator) == -3.5


def test_floordiv_data():
    indicator = Indicator()

    indicator.handle_new_data(2)
    assert (indicator // 5) == 0

    indicator.handle_new_data(16)
    assert (indicator // 5) == 3

    indicator.handle_new_data(-13)
    assert (indicator // 5) == -3


def test_floordiv_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(2)
    other_indicator.handle_new_data(4)
    assert (indicator // other_indicator) == 0

    indicator.handle_new_data(16)
    other_indicator.handle_new_data(5)
    assert (indicator // other_indicator) == 3

    indicator.handle_new_data(7)
    other_indicator.handle_new_data(-2)
    assert (indicator // other_indicator) == -4


def test_neg():
    indicator = Indicator()

    indicator.handle_new_data(2)
    assert -indicator == -2

    indicator.handle_new_data(-3)
    assert -indicator == 3


def test_and_data():
    indicator = Indicator()

    indicator.handle_new_data(True)
    assert (indicator & True) == True

    indicator.handle_new_data(False)
    assert (indicator & True) == False

    derived_indicator = indicator.sand(False)

    indicator.handle_new_data(True)
    assert (indicator & False) == False

    indicator.handle_new_data(False)
    assert (indicator & False) == False


def test_and_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(True)
    other_indicator.handle_new_data(True)
    assert (indicator & other_indicator) == True

    indicator.handle_new_data(True)
    other_indicator.handle_new_data(False)
    assert (indicator & other_indicator) == False

    indicator.handle_new_data(False)
    other_indicator.handle_new_data(True)
    assert (indicator & other_indicator) == False

    indicator.handle_new_data(False)
    other_indicator.handle_new_data(False)
    assert (indicator & other_indicator) == False


def test_or_data():
    indicator = Indicator()

    indicator.handle_new_data(True)
    assert (indicator | True) == True

    indicator.handle_new_data(False)
    assert (indicator | True) == True

    indicator.handle_new_data(True)
    assert (indicator | False) == True

    indicator.handle_new_data(False)
    assert (indicator | False) == False


def test_or_indicator():
    indicator = Indicator()
    other_indicator = Indicator()

    indicator.handle_new_data(True)
    other_indicator.handle_new_data(True)
    assert (indicator | other_indicator) == True

    indicator.handle_new_data(True)
    other_indicator.handle_new_data(False)
    assert (indicator | other_indicator) == True

    indicator.handle_new_data(False)
    other_indicator.handle_new_data(True)
    assert (indicator | other_indicator) == True

    indicator.handle_new_data(False)
    other_indicator.handle_new_data(False)
    assert (indicator | other_indicator) == False


def test_bool():
    indicator = Indicator()

    indicator.handle_new_data(True)
    assert indicator

    indicator.handle_new_data(False)
    assert not indicator


def test_indicator_unary_operation_data_type_not_in_allowed_types():
    indicator = Indicator()
    indicator.handle_new_data(2)
    with pytest.raises(Exception):
        indicator.indicator_unary_operation(
            operator_function=operator.__neg__, allowed_types=[int, float]
        )


def test_indicator_unary_operation_data_is_none():
    indicator = Indicator()
    derived_indicator = indicator.indicator_unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert id(derived_indicator.source_indicator) == id(indicator)
    assert derived_indicator.data is None


def test_indicator_unary_operation_data_is_not_none_and_allowed():
    indicator = Indicator()
    indicator.handle_new_data(2)
    derived_indicator = indicator.indicator_unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert id(derived_indicator.source_indicator) == id(indicator)
    assert derived_indicator.data == -2


def test_indicator_unary_operation_propagation():
    indicator = Indicator()
    indicator.handle_new_data(2)
    derived_indicator = indicator.indicator_unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert id(derived_indicator.source_indicator) == id(indicator)
    assert derived_indicator.data == -2

    indicator.handle_new_data(-5)
    assert derived_indicator.data == 5


def test_indicator_binary_operation_indicator_type_not_in_allowed_types():
    indicator = Indicator()
    indicator.handle_new_data(2)
    with pytest.raises(Exception):
        indicator.indicator_binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_indicator_binary_operation_other_data_type_not_in_allowed_types():
    indicator = Indicator()
    indicator.handle_new_data(2)

    with pytest.raises(Exception):
        indicator.indicator_binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_indicator_binary_operation_other_indicator_type_not_in_allowed_types():
    indicator = Indicator()
    indicator.handle_new_data(2)

    other_indicator = Indicator()
    other_indicator.handle_new_data(5)

    with pytest.raises(Exception):
        indicator.indicator_binary_operation(
            other_indicator,
            operator_function=operator.__add__,
            allowed_types=[int, float],
        )


def test_indicator_binary_operation_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.indicator_binary_operation(
        other_indicator, operator.__add__, allowed_types=[int, float]
    )

    assert derived_indicator.data is None

    # the derived indicator is a "zip" meaning it pairs values 2 by 2
    indicator.on_next(5)
    assert derived_indicator.data is None

    other_indicator.on_next(7)
    assert derived_indicator.data == 12


def test_indicator_binary_operation_data():
    indicator = Indicator()
    derived_indicator = indicator.indicator_binary_operation(
        6, operator.__add__, allowed_types=[int, float]
    )

    assert derived_indicator.data is None

    indicator.on_next(5)
    assert derived_indicator.data == 11


def test_indicator_lt_data():
    indicator = Indicator()
    derived_indicator = indicator.lt(5)

    indicator.on_next(2)
    assert derived_indicator.data == True

    indicator.on_next(5)
    assert derived_indicator.data == False

    indicator.on_next(7)
    assert derived_indicator.data == False


def test_indicator_lt_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.lt(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == True

    indicator.on_next(5)
    other_indicator.on_next(5)
    assert derived_indicator.data == False

    indicator.on_next(8)
    other_indicator.on_next(6)
    assert derived_indicator.data == False


def test_indicator_le_data():
    indicator = Indicator()
    derived_indicator = indicator.le(5)

    indicator.on_next(2)
    assert derived_indicator.data == True

    indicator.on_next(5)
    assert derived_indicator.data == True

    indicator.on_next(7)
    assert derived_indicator.data == False


def test_indicator_le_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.le(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == True

    indicator.on_next(5)
    other_indicator.on_next(5)
    assert derived_indicator.data == True

    indicator.on_next(8)
    other_indicator.on_next(6)
    assert derived_indicator.data == False


def test_indicator_eq_data():
    indicator = Indicator()
    derived_indicator = indicator.eq(5)

    indicator.on_next(2)
    assert derived_indicator.data == False

    indicator.on_next(5)
    assert derived_indicator.data == True


def test_indicator_eq_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.eq(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == False

    indicator.on_next(5)
    other_indicator.on_next(5)
    assert derived_indicator.data == True


def test_indicator_ne_data():
    indicator = Indicator()
    derived_indicator = indicator.ne(5)

    indicator.on_next(2)
    assert derived_indicator.data == True

    indicator.on_next(5)
    assert derived_indicator.data == False


def test_indicator_ne_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.ne(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == True

    indicator.on_next(5)
    other_indicator.on_next(5)
    assert derived_indicator.data == False


def test_indicator_ge_data():
    indicator = Indicator()
    derived_indicator = indicator.ge(5)

    indicator.on_next(2)
    assert derived_indicator.data == False

    indicator.on_next(5)
    assert derived_indicator.data == True

    indicator.on_next(7)
    assert derived_indicator.data == True


def test_indicator_ge_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.ge(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == False

    indicator.on_next(5)
    other_indicator.on_next(5)
    assert derived_indicator.data == True

    indicator.on_next(8)
    other_indicator.on_next(6)
    assert derived_indicator.data == True


def test_indicator_gt_data():
    indicator = Indicator()
    derived_indicator = indicator.gt(5)

    indicator.on_next(2)
    assert derived_indicator.data == False

    indicator.on_next(5)
    assert derived_indicator.data == False

    indicator.on_next(7)
    assert derived_indicator.data == True


def test_indicator_gt_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.gt(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == False

    indicator.on_next(5)
    other_indicator.on_next(5)
    assert derived_indicator.data == False

    indicator.on_next(8)
    other_indicator.on_next(6)
    assert derived_indicator.data == True


def test_indicator_add_data():
    indicator = Indicator()
    derived_indicator = indicator.add(5)

    indicator.on_next(2)
    assert derived_indicator.data == 7

    indicator.on_next(5)
    assert derived_indicator.data == 10


def test_indicator_add_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.add(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == 6

    indicator.on_next(7)
    other_indicator.on_next(2)
    assert derived_indicator.data == 9


def test_iadd_data():
    indicator = Indicator()
    initial_indicator = indicator
    indicator += 5

    initial_indicator.on_next(2)
    assert indicator.data == 7

    initial_indicator.on_next(5)
    assert indicator.data == 10


def test_iadd_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    initial_indicator = indicator
    indicator += other_indicator

    initial_indicator.on_next(2)
    other_indicator.on_next(4)
    assert indicator.data == 6

    initial_indicator.on_next(7)
    other_indicator.on_next(2)
    assert indicator.data == 9


def test_indicator_sub_data():
    indicator = Indicator()
    derived_indicator = indicator.sub(5)

    indicator.on_next(2)
    assert derived_indicator.data == -3

    indicator.on_next(5)
    assert derived_indicator.data == 0


def test_indicator_sub_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.sub(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == -2

    indicator.on_next(7)
    other_indicator.on_next(2)
    assert derived_indicator.data == 5


def test_isub_data():
    indicator = Indicator()
    initial_indicator = indicator
    indicator -= 5

    initial_indicator.on_next(2)
    assert indicator.data == -3

    initial_indicator.on_next(5)
    assert indicator.data == 0


def test_isub_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    initial_indicator = indicator
    indicator -= other_indicator

    initial_indicator.on_next(2)
    other_indicator.on_next(4)
    assert indicator.data == -2

    initial_indicator.on_next(7)
    other_indicator.on_next(2)
    assert indicator.data == 5


def test_indicator_mul_data():
    indicator = Indicator()
    derived_indicator = indicator.mul(5)

    indicator.on_next(2)
    assert derived_indicator.data == 10

    indicator.on_next(-5)
    assert derived_indicator.data == -25


def test_indicator_mul_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.mul(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == 8

    indicator.on_next(7)
    other_indicator.on_next(-2)
    assert derived_indicator.data == -14


def test_imul_data():
    indicator = Indicator()
    initial_indicator = indicator
    indicator *= 5

    initial_indicator.on_next(2)
    assert indicator.data == 10

    initial_indicator.on_next(-5)
    assert indicator.data == -25


def test_imul_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    initial_indicator = indicator
    indicator *= other_indicator

    initial_indicator.on_next(2)
    other_indicator.on_next(4)
    assert indicator.data == 8

    initial_indicator.on_next(7)
    other_indicator.on_next(-2)
    assert indicator.data == -14


def test_indicator_truediv_data():
    indicator = Indicator()
    derived_indicator = indicator.truediv(5)

    indicator.on_next(2)
    assert derived_indicator.data == 0.4

    indicator.on_next(-5)
    assert derived_indicator.data == -1.0


def test_indicator_truediv_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.truediv(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == 0.5

    indicator.on_next(7)
    other_indicator.on_next(-2)
    assert derived_indicator.data == -3.5


def test_itruediv_data():
    indicator = Indicator()
    initial_indicator = indicator
    indicator /= 5

    initial_indicator.on_next(2)
    assert indicator.data == 0.4

    initial_indicator.on_next(-5)
    assert indicator.data == -1.0


def test_itruediv_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    initial_indicator = indicator
    indicator /= other_indicator

    initial_indicator.on_next(2)
    other_indicator.on_next(4)
    assert indicator.data == 0.5

    initial_indicator.on_next(7)
    other_indicator.on_next(-2)
    assert indicator.data == -3.5


def test_indicator_floordiv_data():
    indicator = Indicator()
    derived_indicator = indicator.floordiv(5)

    indicator.on_next(2)
    assert derived_indicator.data == 0

    indicator.on_next(16)
    assert derived_indicator.data == 3

    indicator.on_next(-13)
    assert derived_indicator.data == -3


def test_indicator_floordiv_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.floordiv(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == 0

    indicator.on_next(16)
    other_indicator.on_next(5)
    assert derived_indicator.data == 3

    indicator.on_next(7)
    other_indicator.on_next(-2)
    assert derived_indicator.data == -4


def test_ifloordiv_data():
    indicator = Indicator()
    initial_indicator = indicator
    indicator //= 5

    initial_indicator.on_next(2)
    assert indicator.data == 0

    initial_indicator.on_next(16)
    assert indicator.data == 3

    initial_indicator.on_next(-14)
    assert indicator.data == -3


def test_ifloordiv_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    initial_indicator = indicator
    indicator //= other_indicator

    initial_indicator.on_next(2)
    other_indicator.on_next(4)
    assert indicator.data == 0

    initial_indicator.on_next(16)
    other_indicator.on_next(5)
    assert indicator.data == 3

    initial_indicator.on_next(7)
    other_indicator.on_next(-2)
    assert indicator.data == -4


def test_indicator_neg():
    indicator = Indicator()
    derived_indicator = indicator.neg()

    indicator.on_next(2)
    assert derived_indicator.data == -2

    indicator.on_next(-3)
    assert derived_indicator.data == 3


def test_indicator_and_data():
    indicator = Indicator()
    derived_indicator = indicator.sand(True)

    indicator.on_next(True)
    assert derived_indicator.data == True

    indicator.on_next(False)
    assert derived_indicator.data == False

    derived_indicator = indicator.sand(False)

    indicator.on_next(True)
    assert derived_indicator.data == False

    indicator.on_next(False)
    assert derived_indicator.data == False


def test_indicator_and_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.sand(other_indicator)

    indicator.on_next(True)
    other_indicator.on_next(True)
    assert derived_indicator.data == True

    indicator.on_next(True)
    other_indicator.on_next(False)
    assert derived_indicator.data == False

    indicator.on_next(False)
    other_indicator.on_next(True)
    assert derived_indicator.data == False

    indicator.on_next(False)
    other_indicator.on_next(False)
    assert derived_indicator.data == False


def test_iand_data():
    indicator = Indicator()
    initial_indicator = indicator
    indicator &= True

    initial_indicator.on_next(False)
    assert indicator.data == False

    initial_indicator.on_next(True)
    assert indicator.data == True

    initial_indicator = indicator
    indicator &= False

    initial_indicator.on_next(False)
    assert indicator.data == False

    initial_indicator.on_next(True)
    assert indicator.data == False


def test_iand_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    initial_indicator = indicator
    indicator &= other_indicator

    initial_indicator.on_next(True)
    other_indicator.on_next(True)
    assert indicator.data == True

    initial_indicator.on_next(True)
    other_indicator.on_next(False)
    assert indicator.data == False

    initial_indicator.on_next(False)
    other_indicator.on_next(True)
    assert indicator.data == False

    initial_indicator.on_next(False)
    other_indicator.on_next(False)
    assert indicator.data == False


def test_indicator_or_data():
    indicator = Indicator()
    derived_indicator = indicator.sor(True)

    indicator.on_next(True)
    assert derived_indicator.data == True

    indicator.on_next(False)
    assert derived_indicator.data == True

    derived_indicator = indicator.sor(False)

    indicator.on_next(True)
    assert derived_indicator.data == True

    indicator.on_next(False)
    assert derived_indicator.data == False


def test_indicator_or_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    derived_indicator = indicator.sor(other_indicator)

    indicator.on_next(True)
    other_indicator.on_next(True)
    assert derived_indicator.data == True

    indicator.on_next(True)
    other_indicator.on_next(False)
    assert derived_indicator.data == True

    indicator.on_next(False)
    other_indicator.on_next(True)
    assert derived_indicator.data == True

    indicator.on_next(False)
    other_indicator.on_next(False)
    assert derived_indicator.data == False


def test_ior_data():
    indicator = Indicator()
    initial_indicator = indicator
    indicator |= True

    initial_indicator.on_next(False)
    assert indicator.data == True

    initial_indicator.on_next(True)
    assert indicator.data == True

    initial_indicator = indicator
    indicator |= False

    initial_indicator.on_next(False)
    assert indicator.data == False

    initial_indicator.on_next(True)
    assert indicator.data == True


def test_ior_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    initial_indicator = indicator
    indicator |= other_indicator

    initial_indicator.on_next(True)
    other_indicator.on_next(True)
    assert indicator.data == True

    initial_indicator.on_next(True)
    other_indicator.on_next(False)
    assert indicator.data == True

    initial_indicator.on_next(False)
    other_indicator.on_next(True)
    assert indicator.data == True

    initial_indicator.on_next(False)
    other_indicator.on_next(False)
    assert indicator.data == False


def test_zip_indicator():
    indicator = Indicator()
    other_indicator = Indicator()
    zip_indicator = ZipIndicator(
        source_indicator1=indicator,
        source_indicator2=other_indicator,
        operation_function=operator.__add__,
    )

    other_indicator.on_next(None)
    assert zip_indicator.data is None
    indicator.on_next(None)
    assert zip_indicator.data is None

    indicator.on_next(2)
    assert zip_indicator.data is None
    indicator.on_next(3)
    assert zip_indicator.data is None
    indicator.on_next(4)
    assert zip_indicator.data is None

    other_indicator.on_next(5)
    assert zip_indicator.data == 7
    other_indicator.on_next(6)
    assert zip_indicator.data == 9
    other_indicator.on_next(7)
    assert zip_indicator.data == 11

    other_indicator.on_next(8)
    assert zip_indicator.data == 11
