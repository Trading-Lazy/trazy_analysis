import os
from typing import Dict, Optional, Type

import ccxt
from ccxt.base.exchange import Exchange

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.broker.ccxt_parser import Parser
from trazy_analysis.broker.fee_model import FeeModel

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class CcxtConnector:
    FORMAT_FUNC = {"binance": lambda exchange_symbol: exchange_symbol}

    def __init__(
        self,
        exchanges_api_keys: Dict[str, str],
        parsers: Dict[str, Parser] = None,
        fee_models: Dict[str, FeeModel] = None,
    ):
        self.exchanges_api_keys = exchanges_api_keys
        self.exchanges = list(exchanges_api_keys.keys())
        self.authorized_exchanges = ccxt.exchanges
        self.parsers = parsers
        self.fee_models = fee_models
        self.mapping_last_update = None

        self.exchanges_instances = {}
        for exchange in exchanges_api_keys:
            # initialize instances
            key = exchanges_api_keys[exchange]["key"]
            secret = exchanges_api_keys[exchange]["secret"]
            password = exchanges_api_keys[exchange]["password"]

            exchange_to_lower = exchange.lower()
            if exchange_to_lower not in self.authorized_exchanges:
                LOG.error(
                    f"Invalid exchange %s not among authorized ccxt exchanges %s, "
                    f"no connection will be made to this exchange",
                    exchange_to_lower,
                    self.authorized_exchanges,
                )
                continue
            exchange_class = getattr(ccxt, exchange_to_lower)
            exchange_instance = exchange_class(
                {"apiKey": key, "secret": secret, "password": password}
            )
            self.exchanges_instances[exchange_to_lower] = exchange_instance

    def get_exchange_instance(self, exchange: str) -> Optional[Exchange]:
        if exchange not in self.exchanges_instances:
            LOG.error(
                "Exchange %s is not among the list of managed exchanges: %s",
                exchange,
                list(self.exchanges_instances.keys()),
            )
            return None
        return self.exchanges_instances[exchange]

    def get_parser(self, exchange: str) -> Optional[Type[Parser]]:
        if exchange not in self.parsers:
            LOG.error(
                "Exchange %s is not among the list of parsers: %s",
                exchange,
                list(self.parsers.keys()),
            )
            return None
        return self.parsers[exchange]

    def get_fee_model(self, exchange: str) -> Optional[FeeModel]:
        if exchange not in self.fee_models:
            LOG.error(
                "Exchange %s is not among the list of parsers: %s",
                exchange,
                list(self.fee_models.keys()),
            )
            return None
        return self.fee_models[exchange]

    def format_symbol(self, exchange: str, symbol: str) -> str:
        return CcxtConnector.FORMAT_FUNC[exchange](symbol)
