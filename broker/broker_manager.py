from typing import Dict

from trazy_analysis.broker.broker import Broker
from trazy_analysis.common.clock import Clock


class BrokerManager:
    def __init__(self, brokers: Dict[str, Broker], clock: Clock) -> None:
        self.brokers = brokers
        self.clock = clock

    def add_broker(self, exchange: str, broker: Broker) -> None:
        self.brokers[exchange] = broker

    def get_broker(self, exchange: str) -> Broker:
        return self.brokers[exchange]
