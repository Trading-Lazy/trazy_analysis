from trazy_analysis.bot.bot import get_strategies_classes
from trazy_analysis.strategy.strategy import StrategyBase


def test_get_strategies_classes():
    strategies_module_path = "../strategy/strategies"
    strategies_module_fullname = "strategy.strategies"
    strategies_classes = get_strategies_classes(
        strategies_module_path, strategies_module_fullname
    )

    for strategy_class in strategies_classes:
        assert issubclass(strategy_class, StrategyBase)
    assert len(set(strategies_classes)) == len(strategies_classes)
