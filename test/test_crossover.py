import pytest

from trazy_analysis.indicators.crossover import CrossoverState
from trazy_analysis.indicators.indicators_managers import ReactiveIndicators

indicators = ReactiveIndicators(memoize=False)


@pytest.fixture
def test_crossover_fixture_neg():
    stream_data1 = indicators.Indicator(size=1)
    stream_data2 = indicators.Indicator(size=1)
    crossover = indicators.Crossover(stream_data1, stream_data2)
    stream_data1.next(2)
    stream_data2.next(3)
    return stream_data1, stream_data2, crossover


@pytest.fixture
def test_crossover_fixture_pos():
    stream_data1 = indicators.Indicator(size=1)
    stream_data2 = indicators.Indicator(size=1)
    crossover = indicators.Crossover(stream_data1, stream_data2)
    stream_data1.next(3)
    stream_data2.next(2)
    return stream_data1, stream_data2, crossover


@pytest.fixture
def test_crossover_fixture_idle():
    stream_data1 = indicators.Indicator(size=1)
    stream_data2 = indicators.Indicator(size=1)
    crossover = indicators.Crossover(stream_data1, stream_data2)
    return stream_data1, stream_data2, crossover


@pytest.fixture
def test_crossover_fixture_idle_neg_trend():
    stream_data1 = indicators.Indicator(size=1)
    stream_data2 = indicators.Indicator(size=1)
    crossover = indicators.Crossover(stream_data1, stream_data2)
    stream_data1.next(2)
    stream_data2.next(3)
    stream_data1.next(1)
    stream_data2.next(3)
    return stream_data1, stream_data2, crossover


@pytest.fixture
def test_crossover_fixture_idle_pos_trend():
    stream_data1 = indicators.Indicator(size=1)
    stream_data2 = indicators.Indicator(size=1)
    crossover = indicators.Crossover(stream_data1, stream_data2)
    stream_data1.next(3)
    stream_data2.next(2)
    stream_data1.next(3)
    stream_data2.next(1)
    return stream_data1, stream_data2, crossover


def test_crossover_first_value_pos(test_crossover_fixture_idle):
    stream_data1, stream_data2, crossover = test_crossover_fixture_idle
    assert crossover.data is None
    stream_data1.next(3)
    stream_data2.next(2)
    assert crossover.state == CrossoverState.IDLE_POS_TREND
    assert crossover.data == 0


def test_crossover_first_value_neg(test_crossover_fixture_idle):
    stream_data1, stream_data2, crossover = test_crossover_fixture_idle
    assert crossover.data is None
    stream_data1.next(2)
    stream_data2.next(3)
    assert crossover.state == CrossoverState.IDLE_NEG_TREND
    assert crossover.data == 0


def test_crossover_first_value_idle(test_crossover_fixture_idle):
    stream_data1, stream_data2, crossover = test_crossover_fixture_idle
    assert crossover.data is None
    stream_data1.next(3)
    stream_data2.next(2)
    assert crossover.state == CrossoverState.IDLE_POS_TREND
    assert crossover.data == 0


def test_crossover1(test_crossover_fixture_neg):
    stream_data1, stream_data2, crossover = test_crossover_fixture_neg

    stream_data1.next(3)
    stream_data2.next(2)
    assert crossover.state == CrossoverState.POS
    assert crossover.data == 1


def test_crossover2(test_crossover_fixture_neg):
    stream_data1, stream_data2, crossover = test_crossover_fixture_neg

    stream_data1.next(2)
    stream_data2.next(3)
    assert crossover.state == CrossoverState.IDLE_NEG_TREND
    assert crossover.data == 0


def test_crossover3(test_crossover_fixture_neg):
    stream_data1, stream_data2, crossover = test_crossover_fixture_neg

    stream_data1.next(3)
    stream_data2.next(3)
    assert crossover.state == CrossoverState.POS
    assert crossover.data == 1


def test_crossover4(test_crossover_fixture_pos):
    stream_data1, stream_data2, crossover = test_crossover_fixture_pos

    stream_data1.next(3)
    stream_data2.next(2)
    assert crossover.state == CrossoverState.IDLE_POS_TREND
    assert crossover.data == 0


def test_crossover5(test_crossover_fixture_pos):
    stream_data1, stream_data2, crossover = test_crossover_fixture_pos

    stream_data1.next(2)
    stream_data2.next(3)
    assert crossover.state == CrossoverState.NEG
    assert crossover.data == -1


def test_crossover6(test_crossover_fixture_pos):
    stream_data1, stream_data2, crossover = test_crossover_fixture_pos

    stream_data1.next(3)
    stream_data2.next(3)
    assert crossover.state == CrossoverState.IDLE_POS_TREND
    assert crossover.data == 0


def test_crossover7(test_crossover_fixture_idle_pos_trend):
    stream_data1, stream_data2, crossover = test_crossover_fixture_idle_pos_trend

    stream_data1.next(3)
    stream_data2.next(2)
    assert crossover.state == CrossoverState.IDLE_POS_TREND
    assert crossover.data == 0


def test_crossover8(test_crossover_fixture_idle_pos_trend):
    stream_data1, stream_data2, crossover = test_crossover_fixture_idle_pos_trend

    stream_data1.next(2)
    stream_data2.next(3)
    assert crossover.state == CrossoverState.NEG
    assert crossover.data == -1


def test_crossover9(test_crossover_fixture_idle_pos_trend):
    stream_data1, stream_data2, crossover = test_crossover_fixture_idle_pos_trend

    stream_data1.next(3)
    stream_data2.next(3)
    assert crossover.state == CrossoverState.IDLE_POS_TREND
    assert crossover.data == 0


def test_crossover10(test_crossover_fixture_idle_neg_trend):
    stream_data1, stream_data2, crossover = test_crossover_fixture_idle_neg_trend

    stream_data1.next(2)
    stream_data2.next(3)
    assert crossover.state == CrossoverState.IDLE_NEG_TREND
    assert crossover.data == 0


def test_crossover11(test_crossover_fixture_idle_neg_trend):
    stream_data1, stream_data2, crossover = test_crossover_fixture_idle_neg_trend

    stream_data1.next(3)
    stream_data2.next(2)
    assert crossover.state == CrossoverState.POS
    assert crossover.data == 1


def test_crossover12(test_crossover_fixture_idle_neg_trend):
    stream_data1, stream_data2, crossover = test_crossover_fixture_idle_neg_trend

    stream_data1.next(3)
    stream_data2.next(3)
    assert crossover.state == CrossoverState.POS
    assert crossover.data == 1
