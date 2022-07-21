from datetime import timedelta
from typing import Dict, Any

from trazy_analysis.broker.broker import Broker
from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.models.enums import BrokerIsolation


class BrokerManager:
    def __init__(
        self,
        isolation: BrokerIsolation = BrokerIsolation.EXCHANGE,
        brokers: dict[Any, Any] = {},
    ) -> None:
        self.brokers = brokers
        self.isolation = isolation

    def set_broker(
        self,
        broker: Broker,
        exchange: str = None,
        symbol: str = None,
        strategy_name: str = None,
    ) -> None:
        match self.isolation:
            case BrokerIsolation.EXCHANGE:
                self.brokers[exchange] = broker
            case BrokerIsolation.ASSET:
                get_or_create_nested_dict(self.brokers, exchange)
                self.brokers[exchange][symbol] = broker
            case BrokerIsolation.STRATEGY:
                self.brokers[strategy_name] = broker
            case BrokerIsolation.STRATEGY_AND_EXCHANGE:
                get_or_create_nested_dict(self.brokers, strategy_name)
                self.brokers[strategy_name][exchange] = broker
            case BrokerIsolation.STRATEGY_AND_ASSET:
                get_or_create_nested_dict(self.brokers, strategy_name, exchange)
                self.brokers[strategy_name][exchange][symbol] = broker

    def get_broker(
        self, exchange: str = None, symbol: str = None, strategy_name: str = None
    ) -> Broker:
        match self.isolation:
            case BrokerIsolation.EXCHANGE:
                return self.brokers[exchange]
            case BrokerIsolation.ASSET:
                return self.brokers[exchange][symbol]
            case BrokerIsolation.STRATEGY:
                return self.brokers[strategy_name]
            case BrokerIsolation.STRATEGY_AND_EXCHANGE:
                return self.brokers[strategy_name][exchange]
            case BrokerIsolation.STRATEGY_AND_ASSET:
                return self.brokers[strategy_name][exchange][symbol]
