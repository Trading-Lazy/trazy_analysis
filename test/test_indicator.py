import operator

import pytest

from trazy_analysis.common.meta import IndicatorMemoization
from trazy_analysis.indicators.indicators_managers import ReactiveIndicators
from trazy_analysis.models.enums import ExecutionMode

indicators = ReactiveIndicators(memoize=False, mode=ExecutionMode.LIVE)


def test_handle_new_data_default_transform():
    indicator = indicators.Indicator(size=1)

    new_data = 3
    indicator.handle_data(new_data)

    assert indicator.data == new_data


def test_handle_new_data_custom_transform():
    indicator = indicators.Indicator(size=1, transform=lambda x: x * 2)

    indicator.handle_data(5)

    assert indicator.data == 10


def test_ignore_source_indicator_is_not_none():
    source_indicator = indicators.Indicator(size=1)
    indicator = indicators.Indicator(source=source_indicator, size=1)
    assert len(source_indicator.callbacks) == 1
    indicator.ignore()
    assert len(source_indicator.callbacks) == 0


def test_ignore_source_indicator_is_none():
    indicator = indicators.Indicator(size=1)
    assert len(indicator.callbacks) == 0
    indicator.ignore()
    assert len(indicator.callbacks) == 0


def test_observe_source_indicator_is_none():
    source_indicator = indicators.Indicator(size=1)
    indicator = indicators.Indicator(source=source_indicator, size=1)
    indicator.observe(None)
    assert indicator.source is None
    assert indicator.data is None


def test_observe_source_indicator_is_indicator_and_has_no_data():
    indicator = indicators.Indicator(size=1)

    source_indicator = indicators.Indicator(size=1)
    indicator.observe(source_indicator)

    assert id(indicator.source) == id(source_indicator)
    assert indicator.data is None


def test_observe_source_indicator_is_indicator_and_has_data():
    indicator = indicators.Indicator(size=1, transform=lambda x: x * 2)
    source_indicator = indicators.Indicator(size=1)
    source_indicator.push(5)
    indicator.observe(source_indicator)

    assert id(indicator.source) == id(source_indicator)
    assert indicator.data is None
    source_indicator.push(7)
    assert indicator.data == source_indicator.data * 2


def test_observe_source_indicator_is_observable_and_has_no_data():
    indicator = indicators.Indicator(size=1)

    source_indicator = indicators.Indicator(size=1)
    indicator.observe(source_indicator)

    assert id(indicator.source) == id(source_indicator)

    assert indicator.data is None


def test_observe_source_indicator_is_observable_and_has_data():
    indicator = indicators.Indicator(size=1, transform=lambda x: x * 2)

    source_indicator = indicators.Indicator(size=1)
    source_indicator.handle_data(-6)

    indicator.observe(source_indicator)
    assert id(indicator.source) == id(source_indicator)
    assert indicator.data is None
    source_indicator.handle_data(-7)
    assert indicator.data == -14


def test_observe_source_indicator_is_indicator_propagation():
    indicator = indicators.Indicator(size=1, transform=lambda x: x * 2)

    source_indicator = indicators.Indicator(size=1)
    indicator.observe(source_indicator)

    source_indicator.on_next(3)
    assert indicator.data == 6

    source_indicator.on_next(-5)
    assert indicator.data == -10


def test_observe_source_indicator_is_observable_data_propagation():
    indicator = indicators.Indicator(size=1, transform=lambda x: x * 2)

    source_indicator = indicators.Indicator(size=1)
    source_indicator.handle_data(-6)
    indicator.observe(source_indicator)

    assert indicator.data is None

    source_indicator.handle_data(-6)
    assert indicator.data == -12


def test_unary_operation_data_type_not_in_allowed_types():
    indicator = indicators.Indicator(size=1)
    indicator.handle_data(2)
    with pytest.raises(Exception):
        indicator.unary_operation(
            operator_function=operator.__neg__, allowed_types=[int, float]
        )


def test_unary_operation_data_is_none():
    indicator = indicators.Indicator(size=1)
    unary_operation_result = indicator.unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert unary_operation_result is None


def test_unary_operation_data_is_not_none_and_allowed():
    indicator = indicators.Indicator(size=1)
    indicator.handle_data(2)
    unary_operation_result = indicator.unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert unary_operation_result == -2


def test_binary_operation_indicator_type_not_in_allowed_types():
    indicator = indicators.Indicator(size=1)
    indicator.handle_data(2)
    with pytest.raises(Exception):
        indicator.binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_binary_operation_other_data_type_not_in_allowed_types():
    indicator = indicators.Indicator(size=1)
    indicator.handle_data(2)

    with pytest.raises(Exception):
        indicator.binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_binary_operation_other_indicator_type_not_in_allowed_types():
    indicator = indicators.Indicator(size=1)
    indicator.handle_data(2)

    other_indicator = indicators.Indicator(size=1)
    other_indicator.handle_data(5)

    with pytest.raises(Exception):
        indicator.binary_operation(
            other_indicator,
            operator_function=operator.__add__,
            allowed_types=[int, float],
        )


def test_binary_operation_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    binary_operation_result = indicator.binary_operation(
        other_indicator, operator.__add__, allowed_types=[int, float]
    )

    assert binary_operation_result is None

    # the derived indicator is a "zip" meaning it pairs values 2 by 2
    indicator.handle_data(5)
    binary_operation_result = indicator.binary_operation(
        other_indicator, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation_result is None

    other_indicator.handle_data(7)
    binary_operation_result = indicator.binary_operation(
        other_indicator, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation_result == 12


def test_binary_operation_data():
    indicator = indicators.Indicator(size=1)
    binary_operation = indicator.binary_operation(
        6, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation is None

    indicator.handle_data(5)
    binary_operation = indicator.binary_operation(
        6, operator.__add__, allowed_types=[int, float]
    )
    assert binary_operation == 11


def test_lt_data():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    assert indicator < 5

    indicator.handle_data(5)
    assert not (indicator < 5)

    indicator.handle_data(7)
    assert not (indicator < 5)


def test_lt_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    other_indicator.handle_data(4)
    assert indicator < other_indicator

    indicator.handle_data(5)
    other_indicator.handle_data(5)
    assert not (indicator < other_indicator)

    indicator.handle_data(8)
    other_indicator.handle_data(6)
    assert not (indicator < other_indicator)


def test_le_data():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    assert indicator <= 5

    indicator.handle_data(5)
    assert indicator <= 5

    indicator.handle_data(7)
    assert not (indicator <= 5)


def test_le_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    other_indicator.handle_data(4)
    assert indicator <= other_indicator

    indicator.handle_data(5)
    other_indicator.handle_data(5)
    assert indicator <= other_indicator

    indicator.handle_data(8)
    other_indicator.handle_data(6)
    assert not (indicator <= other_indicator)


def test_eq_data():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    assert not (indicator == 5)

    indicator.handle_data(5)
    assert indicator == 5


def test_eq_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    other_indicator.handle_data(4)
    assert not (indicator == other_indicator)

    indicator.handle_data(5)
    other_indicator.handle_data(5)
    assert indicator == other_indicator


def test_ne_data():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    assert indicator != 5

    indicator.handle_data(5)
    assert not (indicator != 5)


def test_ne_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    other_indicator.handle_data(4)
    assert indicator != other_indicator

    indicator.handle_data(5)
    other_indicator.handle_data(5)
    assert not (indicator != other_indicator)


def test_ge_data():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    assert not (indicator >= 5)

    indicator.handle_data(5)
    assert indicator >= 5

    indicator.handle_data(7)
    assert indicator >= 5


def test_ge_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    other_indicator.handle_data(4)
    assert not (indicator >= other_indicator)

    indicator.handle_data(5)
    other_indicator.handle_data(5)
    assert indicator >= other_indicator

    indicator.handle_data(8)
    other_indicator.handle_data(6)
    assert indicator >= other_indicator


def test_gt_data():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    assert not (indicator > 5)

    indicator.handle_data(5)
    assert not (indicator > 5)

    indicator.handle_data(7)
    assert indicator > 5


def test_gt_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    other_indicator.handle_data(4)
    assert not (indicator > other_indicator)

    indicator.handle_data(5)
    other_indicator.handle_data(5)
    assert not (indicator > other_indicator)

    indicator.handle_data(8)
    other_indicator.handle_data(6)
    assert indicator > other_indicator


def test_add_data():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    assert (indicator + 5) == 7

    indicator.handle_data(5)
    assert (indicator + 5) == 10


def test_add_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    other_indicator.handle_data(4)
    assert (indicator + other_indicator) == 6

    indicator.handle_data(7)
    other_indicator.handle_data(2)
    assert (indicator + other_indicator) == 9


def test_sub_data():
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator - 5

    indicator.handle_data(2)
    assert (indicator - 5) == -3

    indicator.handle_data(5)
    assert (indicator - 5) == 0


def test_sub_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    derived_indicator = indicator - other_indicator

    indicator.handle_data(2)
    other_indicator.handle_data(4)
    assert (indicator - other_indicator) == -2

    indicator.handle_data(7)
    other_indicator.handle_data(2)
    assert (indicator - other_indicator) == 5


def test_mul_data():
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator * 5

    indicator.handle_data(2)
    assert (indicator * 5) == 10

    indicator.handle_data(-5)
    assert (indicator * 5) == -25


def test_mul_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    other_indicator.handle_data(4)
    assert (indicator * other_indicator) == 8

    indicator.handle_data(7)
    other_indicator.handle_data(-2)
    assert (indicator * other_indicator) == -14


def test_truediv_data():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    assert (indicator / 5) == 0.4

    indicator.handle_data(-5)
    assert (indicator / 5) == -1.0


def test_truediv_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    other_indicator.handle_data(4)
    assert (indicator / other_indicator) == 0.5

    indicator.handle_data(7)
    other_indicator.handle_data(-2)
    assert (indicator / other_indicator) == -3.5


def test_floordiv_data():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    assert (indicator // 5) == 0

    indicator.handle_data(16)
    assert (indicator // 5) == 3

    indicator.handle_data(-13)
    assert (indicator // 5) == -3


def test_floordiv_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    other_indicator.handle_data(4)
    assert (indicator // other_indicator) == 0

    indicator.handle_data(16)
    other_indicator.handle_data(5)
    assert (indicator // other_indicator) == 3

    indicator.handle_data(7)
    other_indicator.handle_data(-2)
    assert (indicator // other_indicator) == -4


def test_neg():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(2)
    assert -indicator == -2

    indicator.handle_data(-3)
    assert -indicator == 3


def test_and_data():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(True)
    assert (indicator & True) == True

    indicator.handle_data(False)
    assert (indicator & True) == False

    derived_indicator = indicator.sand(False)

    indicator.handle_data(True)
    assert (indicator & False) == False

    indicator.handle_data(False)
    assert (indicator & False) == False


def test_and_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(True)
    other_indicator.handle_data(True)
    assert (indicator & other_indicator) == True

    indicator.handle_data(True)
    other_indicator.handle_data(False)
    assert (indicator & other_indicator) == False

    indicator.handle_data(False)
    other_indicator.handle_data(True)
    assert (indicator & other_indicator) == False

    indicator.handle_data(False)
    other_indicator.handle_data(False)
    assert (indicator & other_indicator) == False


def test_or_data():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(True)
    assert (indicator | True) == True

    indicator.handle_data(False)
    assert (indicator | True) == True

    indicator.handle_data(True)
    assert (indicator | False) == True

    indicator.handle_data(False)
    assert (indicator | False) == False


def test_or_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)

    indicator.handle_data(True)
    other_indicator.handle_data(True)
    assert (indicator | other_indicator) == True

    indicator.handle_data(True)
    other_indicator.handle_data(False)
    assert (indicator | other_indicator) == True

    indicator.handle_data(False)
    other_indicator.handle_data(True)
    assert (indicator | other_indicator) == True

    indicator.handle_data(False)
    other_indicator.handle_data(False)
    assert (indicator | other_indicator) == False


def test_bool():
    indicator = indicators.Indicator(size=1)

    indicator.handle_data(True)
    assert indicator

    indicator.handle_data(False)
    assert not indicator


def test_indicator_unary_operation_data_type_not_in_allowed_types():
    indicator = indicators.Indicator(size=1)
    indicator.handle_data(2)
    with pytest.raises(Exception):
        indicator.indicator_unary_operation(
            operator_function=operator.__neg__, allowed_types=[int, float]
        )


def test_indicator_unary_operation_data_is_none():
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.indicator_unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert id(derived_indicator.source) == id(indicator)
    assert derived_indicator.data is None


def test_indicator_unary_operation_data_is_not_none_and_allowed():
    indicator = indicators.Indicator(size=1)
    indicator.handle_data(2)
    derived_indicator = indicator.indicator_unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert id(derived_indicator.source) == id(indicator)
    assert derived_indicator.data == -2


def test_indicator_unary_operation_propagation():
    indicator = indicators.Indicator(size=1)
    indicator.handle_data(2)
    derived_indicator = indicator.indicator_unary_operation(
        operator.__neg__, allowed_types=[int, float]
    )
    assert id(derived_indicator.source) == id(indicator)
    assert derived_indicator.data == -2

    indicator.handle_data(-5)
    assert derived_indicator.data == 5


def test_indicator_binary_operation_indicator_type_not_in_allowed_types():
    indicator = indicators.Indicator(size=1)
    indicator.handle_data(2)
    with pytest.raises(Exception):
        indicator.indicator_binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_indicator_binary_operation_other_data_type_not_in_allowed_types():
    indicator = indicators.Indicator(size=1)
    indicator.handle_data(2)

    with pytest.raises(Exception):
        indicator.indicator_binary_operation(
            2, operator_function=operator.__add__, allowed_types=[int, float]
        )


def test_indicator_binary_operation_other_indicator_type_not_in_allowed_types():
    indicator = indicators.Indicator(size=1)
    indicator.handle_data(2)

    other_indicator = indicators.Indicator(size=1)
    other_indicator.handle_data(5)

    with pytest.raises(Exception):
        indicator.indicator_binary_operation(
            other_indicator,
            operator_function=operator.__add__,
            allowed_types=[int, float],
        )


def test_indicator_binary_operation_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.indicator_binary_operation(
        6, operator.__add__, allowed_types=[int, float]
    )

    assert derived_indicator.data is None

    indicator.on_next(5)
    assert derived_indicator.data == 11


def test_indicator_lt_data():
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.lt(5)

    indicator.on_next(2)
    assert derived_indicator.data == True

    indicator.on_next(5)
    assert derived_indicator.data == False

    indicator.on_next(7)
    assert derived_indicator.data == False


def test_indicator_lt_rolling_window():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.le(5)

    indicator.on_next(2)
    assert derived_indicator.data == True

    indicator.on_next(5)
    assert derived_indicator.data == True

    indicator.on_next(7)
    assert derived_indicator.data == False


def test_indicator_le_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.eq(5)

    indicator.on_next(2)
    assert derived_indicator.data == False

    indicator.on_next(5)
    assert derived_indicator.data == True


def test_indicator_eq_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.eq(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == False

    indicator.on_next(5)
    other_indicator.on_next(5)
    assert derived_indicator.data == True


def test_indicator_ne_data():
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.ne(5)

    indicator.on_next(2)
    assert derived_indicator.data == True

    indicator.on_next(5)
    assert derived_indicator.data == False


def test_indicator_ne_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.ne(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == True

    indicator.on_next(5)
    other_indicator.on_next(5)
    assert derived_indicator.data == False


def test_indicator_ge_data():
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.ge(5)

    indicator.on_next(2)
    assert derived_indicator.data == False

    indicator.on_next(5)
    assert derived_indicator.data == True

    indicator.on_next(7)
    assert derived_indicator.data == True


def test_indicator_ge_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.gt(5)

    indicator.on_next(2)
    assert derived_indicator.data == False

    indicator.on_next(5)
    assert derived_indicator.data == False

    indicator.on_next(7)
    assert derived_indicator.data == True


def test_indicator_gt_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.add(5)

    indicator.on_next(2)
    assert derived_indicator.data == 7

    indicator.on_next(5)
    assert derived_indicator.data == 10


def test_indicator_add_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.add(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == 6

    indicator.on_next(7)
    other_indicator.on_next(2)
    assert derived_indicator.data == 9


def test_iadd_data():
    indicator = indicators.Indicator(size=1)
    initial_indicator = indicator
    indicator += 5

    initial_indicator.on_next(2)
    assert indicator.data == 7

    initial_indicator.on_next(5)
    assert indicator.data == 10


def test_iadd_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    initial_indicator = indicator
    indicator += other_indicator

    initial_indicator.on_next(2)
    other_indicator.on_next(4)
    assert indicator.data == 6

    initial_indicator.on_next(7)
    other_indicator.on_next(2)
    assert indicator.data == 9


def test_indicator_sub_data():
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.sub(5)

    indicator.on_next(2)
    assert derived_indicator.data == -3

    indicator.on_next(5)
    assert derived_indicator.data == 0


def test_indicator_sub_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.sub(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == -2

    indicator.on_next(7)
    other_indicator.on_next(2)
    assert derived_indicator.data == 5


def test_isub_data():
    indicator = indicators.Indicator(size=1)
    initial_indicator = indicator
    indicator -= 5

    initial_indicator.on_next(2)
    assert indicator.data == -3

    initial_indicator.on_next(5)
    assert indicator.data == 0


def test_isub_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    initial_indicator = indicator
    indicator -= other_indicator

    initial_indicator.on_next(2)
    other_indicator.on_next(4)
    assert indicator.data == -2

    initial_indicator.on_next(7)
    other_indicator.on_next(2)
    assert indicator.data == 5


def test_indicator_mul_data():
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.mul(5)

    indicator.on_next(2)
    assert derived_indicator.data == 10

    indicator.on_next(-5)
    assert derived_indicator.data == -25


def test_indicator_mul_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.mul(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == 8

    indicator.on_next(7)
    other_indicator.on_next(-2)
    assert derived_indicator.data == -14


def test_imul_data():
    indicator = indicators.Indicator(size=1)
    initial_indicator = indicator
    indicator *= 5

    initial_indicator.on_next(2)
    assert indicator.data == 10

    initial_indicator.on_next(-5)
    assert indicator.data == -25


def test_imul_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    initial_indicator = indicator
    indicator *= other_indicator

    initial_indicator.on_next(2)
    other_indicator.on_next(4)
    assert indicator.data == 8

    initial_indicator.on_next(7)
    other_indicator.on_next(-2)
    assert indicator.data == -14


def test_indicator_truediv_data():
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.truediv(5)

    indicator.on_next(2)
    assert derived_indicator.data == 0.4

    indicator.on_next(-5)
    assert derived_indicator.data == -1.0


def test_indicator_truediv_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.truediv(other_indicator)

    indicator.on_next(2)
    other_indicator.on_next(4)
    assert derived_indicator.data == 0.5

    indicator.on_next(7)
    other_indicator.on_next(-2)
    assert derived_indicator.data == -3.5


def test_itruediv_data():
    indicator = indicators.Indicator(size=1)
    initial_indicator = indicator
    indicator /= 5

    initial_indicator.on_next(2)
    assert indicator.data == 0.4

    initial_indicator.on_next(-5)
    assert indicator.data == -1.0


def test_itruediv_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    initial_indicator = indicator
    indicator /= other_indicator

    initial_indicator.on_next(2)
    other_indicator.on_next(4)
    assert indicator.data == 0.5

    initial_indicator.on_next(7)
    other_indicator.on_next(-2)
    assert indicator.data == -3.5


def test_indicator_floordiv_data():
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.floordiv(5)

    indicator.on_next(2)
    assert derived_indicator.data == 0

    indicator.on_next(16)
    assert derived_indicator.data == 3

    indicator.on_next(-13)
    assert derived_indicator.data == -3


def test_indicator_floordiv_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
    initial_indicator = indicator
    indicator //= 5

    initial_indicator.on_next(2)
    assert indicator.data == 0

    initial_indicator.on_next(16)
    assert indicator.data == 3

    initial_indicator.on_next(-14)
    assert indicator.data == -3


def test_ifloordiv_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
    derived_indicator = indicator.neg()

    indicator.on_next(2)
    assert derived_indicator.data == -2

    indicator.on_next(-3)
    assert derived_indicator.data == 3


def test_indicator_and_data():
    indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
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
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    initial_indicator = indicator
    indicator |= other_indicator

    initial_indicator.handle_data(True)
    other_indicator.handle_data(True)
    assert indicator.data == True

    initial_indicator.handle_data(True)
    other_indicator.handle_data(False)
    assert indicator.data == True

    initial_indicator.handle_data(False)
    other_indicator.handle_data(True)
    assert indicator.data == True

    initial_indicator.handle_data(False)
    other_indicator.handle_data(False)
    assert indicator.data == False


def test_zip_indicator():
    indicator = indicators.Indicator(size=1)
    other_indicator = indicators.Indicator(size=1)
    zip_indicator = indicators.ZipIndicator(
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


def test_indicator_memoization():
    class A(metaclass=IndicatorMemoization):
        def __init__(self, size, name, period, source):
            self.size = size
            self.name = name
            self.period = period
            self.source = source

    class B(metaclass=IndicatorMemoization):
        def __init__(self, name, size, period, source):
            self.size = size
            self.name = name
            self.period = period
            self.source = source

    class C(metaclass=IndicatorMemoization):
        def __init__(self, name, period, size, source):
            self.size = size
            self.name = name
            self.period = period
            self.source = source

    class D(metaclass=IndicatorMemoization):
        def __init__(self, name, period, source, size):
            self.size = size
            self.name = name
            self.period = period
            self.source = source

    class E(metaclass=IndicatorMemoization):
        def __init__(self, name, period, source, size, memoize=True):
            self.size = size
            self.name = name
            self.period = period
            self.source = source
            self.memoize = memoize

    a1 = A(5, "a", "b", "c")
    a2 = A(10, "a", "b", "c")
    a3 = A(5, "a", "b", "d")
    assert id(a1) == id(a2)
    assert id(a1) != id(a3)
    a4 = A(size=5, name="a", period="b", source="c")
    a5 = A(size=10, name="a", period="b", source="c")
    a6 = A(size=5, name="a", period="b", source="d")
    assert id(a4) == id(a5)
    assert id(a4) != id(a6)

    b1 = B("a", 5, "b", "c")
    b2 = B("a", 10, "b", "c")
    b3 = B("a", 5, "b", "d")
    assert id(b1) == id(b2)
    assert id(b1) != id(b3)
    b4 = B("a", size=5, period="b", source="c")
    b5 = B("a", size=10, period="b", source="c")
    b6 = B("a", size=5, period="b", source="d")
    assert id(b4) == id(b5)
    assert id(b4) != id(b6)

    c1 = C("a", "b", 5, "c")
    c2 = C("a", "b", 10, "c")
    c3 = C("a", "b", 5, "d")
    assert id(c1) == id(c2)
    assert id(c1) != id(c3)
    c4 = C("a", "b", size=5, source="c")
    c5 = C("a", "b", size=10, source="c")
    c6 = C("a", "b", size=5, source="d")
    assert id(c4) == id(c5)
    assert id(c4) != id(c6)

    d1 = D("a", "b", "c", 5)
    d2 = D("a", "b", "c", 10)
    d3 = D("a", "b", "d", 5)
    assert id(d1) == id(d2)
    assert id(d1) != id(d3)
    d4 = D("a", "b", "c", size=5)
    d5 = D("a", "b", "c", size=10)
    d6 = D("a", "b", "d", size=5)
    assert id(d4) == id(d5)
    assert id(d4) != id(d6)

    e1 = E("a", "b", "c", 5)
    e2 = E("a", "b", "c", 5)
    assert e1 != e2
