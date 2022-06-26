from datetime import timedelta
from typing import Dict, Any

from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.models.enums import BrokerIsolation


class StatisticsManager:
    def __init__(
        self,
        isolation: BrokerIsolation = BrokerIsolation.EXCHANGE,
    ) -> None:
        self.equity_curves = {}
        self.equity_dfs = {}
        self.positions = {}
        self.positions_dfs = {}
        self.transactions = {}
        self.transactions_dfs = {}
        self.isolation = isolation

    def set_statistics(
        self,
        statistics_dict: Dict[Any, Any],
        value: Any,
        exchange: str = None,
        symbol: str = None,
        strategy_name: str = None,
    ) -> None:
        if self.isolation == BrokerIsolation.EXCHANGE:
            statistics_dict[exchange] = value
        elif self.isolation == BrokerIsolation.ASSET:
            get_or_create_nested_dict(statistics_dict, exchange)
            statistics_dict[exchange][symbol] = value
        elif self.isolation == BrokerIsolation.STRATEGY:
            statistics_dict[strategy_name] = value
        elif self.isolation == BrokerIsolation.STRATEGY_AND_EXCHANGE:
            get_or_create_nested_dict(statistics_dict, strategy_name)
            statistics_dict[strategy_name][exchange] = value
        elif self.isolation == BrokerIsolation.STRATEGY_AND_ASSET:
            get_or_create_nested_dict(statistics_dict, strategy_name, exchange)
            statistics_dict[strategy_name][exchange][symbol] = value

    def set_equity_curves(
        self,
        value: Any,
        exchange: str = None,
        symbol: str = None,
        strategy_name: str = None,
    ):
        self.set_statistics(self.equity_curves, value, exchange, symbol, strategy_name)

    def set_equity_dfs(
        self,
        value: Any,
        exchange: str = None,
        symbol: str = None,
        strategy_name: str = None,
    ):
        self.set_statistics(self.equity_dfs, value, exchange, symbol, strategy_name)

    def set_positions(
        self,
        value: Any,
        exchange: str = None,
        symbol: str = None,
        strategy_name: str = None,
    ):
        self.set_statistics(self.positions, value, exchange, symbol, strategy_name)

    def set_positions_dfs(
        self,
        value: Any,
        exchange: str = None,
        symbol: str = None,
        strategy_name: str = None,
    ):
        self.set_statistics(self.positions_dfs, value, exchange, symbol, strategy_name)

    def set_transactions(
        self,
        value: Any,
        exchange: str = None,
        symbol: str = None,
        strategy_name: str = None,
    ):
        self.set_statistics(self.transactions, value, exchange, symbol, strategy_name)

    def set_transactions_dfs(
        self,
        value: Any,
        exchange: str = None,
        symbol: str = None,
        strategy_name: str = None,
    ):
        self.set_statistics(
            self.transactions_dfs, value, exchange, symbol, strategy_name
        )

    def get_statistics(
        self,
        statistics_dict: Dict[Any, Any],
        exchange: str = None,
        symbol: str = None,
        strategy_name: str = None,
    ) -> Dict[Any, Any]:
        if self.isolation == BrokerIsolation.EXCHANGE:
            return statistics_dict.get(exchange, None)
        elif self.isolation == BrokerIsolation.ASSET:
            return statistics_dict.get(exchange, {}).get(symbol, None)
        elif self.isolation == BrokerIsolation.STRATEGY:
            return statistics_dict.get(strategy_name, None)
        elif self.isolation == BrokerIsolation.STRATEGY_AND_EXCHANGE:
            return statistics_dict.get(strategy_name, {}).get(exchange, None)
        elif self.isolation == BrokerIsolation.STRATEGY_AND_ASSET:
            return (
                statistics_dict.get(strategy_name, {})
                .get(exchange, {})
                .get(symbol, None)
            )

    def get_equity_curves(
        self, exchange: str = None, symbol: str = None, strategy_name: str = None
    ):
        return self.get_statistics(self.equity_curves, exchange, symbol, strategy_name)

    def get_equity_dfs(
        self, exchange: str = None, symbol: str = None, strategy_name: str = None
    ):
        return self.get_statistics(self.equity_dfs, exchange, symbol, strategy_name)

    def get_positions(
        self, exchange: str = None, symbol: str = None, strategy_name: str = None
    ):
        return self.get_statistics(self.positions, exchange, symbol, strategy_name)

    def get_positions_dfs(
        self, exchange: str = None, symbol: str = None, strategy_name: str = None
    ):
        return self.get_statistics(self.positions_dfs, exchange, symbol, strategy_name)

    def get_transactions(
        self, exchange: str = None, symbol: str = None, strategy_name: str = None
    ):
        return self.get_statistics(self.transactions, exchange, symbol, strategy_name)

    def get_transactions_dfs(
        self, exchange: str = None, symbol: str = None, strategy_name: str = None
    ):
        return self.get_statistics(
            self.transactions_dfs, exchange, symbol, strategy_name
        )
