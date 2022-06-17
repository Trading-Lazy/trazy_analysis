from datetime import timedelta
from typing import Dict, Any

from trazy_analysis.broker.broker import Broker
from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Isolation


class BrokerManager:
    def __init__(
        self, isolation: Isolation = Isolation.EXCHANGE, brokers: Dict[Any, Any] = {}
    ) -> None:
        self.brokers = brokers
        self.isolation = isolation

    def set_broker(
        self,
        broker: Broker,
        exchange: str = None,
        symbol: str = None,
        time_unit: timedelta = None,
        strategy_name: str = None,
    ) -> None:
        if self.isolation == Isolation.EXCHANGE:
            self.brokers[exchange] = broker
        elif self.isolation == Isolation.SYMBOL:
            get_or_create_nested_dict(self.brokers, exchange)
            self.brokers[exchange][symbol] = broker
        elif self.isolation == Isolation.ASSET:
            self.brokers[exchange][symbol][time_unit] = broker
        elif self.isolation == Isolation.STRATEGY:
            self.brokers[strategy_name] = broker
        elif self.isolation == Isolation.STRATEGY_AND_EXCHANGE:
            get_or_create_nested_dict(self.brokers, strategy_name)
            self.brokers[strategy_name][exchange] = broker
        elif self.isolation == Isolation.STRATEGY_AND_SYMBOL:
            get_or_create_nested_dict(self.brokers, strategy_name, exchange)
            self.brokers[strategy_name][exchange][symbol] = broker
        elif self.isolation == Isolation.STRATEGY_AND_ASSET:
            get_or_create_nested_dict(self.brokers, strategy_name)
            self.brokers[strategy_name][exchange][symbol][time_unit] = broker

    def get_broker(
        self,
        exchange: str = None,
        symbol: str = None,
        time_unit: timedelta = None,
        strategy_name: str = None,
    ) -> Broker:
        if self.isolation == Isolation.EXCHANGE:
            return self.brokers[exchange]
        elif self.isolation == Isolation.SYMBOL:
            return self.brokers[exchange][symbol]
        elif self.isolation == Isolation.ASSET:
            return self.brokers[exchange][symbol][time_unit]
        elif self.isolation == Isolation.STRATEGY:
            return self.brokers[strategy_name]
        elif self.isolation == Isolation.STRATEGY_AND_EXCHANGE:
            return self.brokers[strategy_name][exchange]
        elif self.isolation == Isolation.STRATEGY_AND_SYMBOL:
            return self.brokers[strategy_name][exchange][symbol]
        elif self.isolation == Isolation.STRATEGY_AND_ASSET:
            return self.brokers[strategy_name][exchange][symbol][time_unit]
