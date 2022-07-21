import os
import re
from typing import Dict, Optional, Type, Any, Union

import ccxt
from ccxt.base.exchange import Exchange

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.broker.ccxt_parser import Parser
from trazy_analysis.broker.fee_model import FeeModel, FeeModelManager
from trazy_analysis.broker.percent_fee_model import PercentFeeModel
from trazy_analysis.models.asset import Asset

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class CcxtConnector:
    FORMAT_FUNC = {"binance": lambda exchange_symbol: exchange_symbol}
    # Tickers format
    FORMAT1 = re.compile("^[a-zA-Z0-9_]+/[a-zA-Z0-9_]+$")
    FORMAT2 = re.compile("^\.[a-zA-Z0-9_]+$")
    FORMAT3 = re.compile("^\$[a-zA-Z0-9_]+/[a-zA-Z0-9_]+$")
    FORMAT4 = re.compile("^[a-zA-Z0-9_]+-[a-zA-Z0-9_]+$")
    FORMAT5 = re.compile("^[a-zA-Z0-9_]+FP$")
    FORMAT6 = re.compile("^[a-zA-Z0-9_]+_BQX$")
    FORMAT7 = re.compile("^[a-zA-Z0-9_]+_[0-9]{6}$")
    FORMAT8 = re.compile("^CMT_[a-zA-Z0-9_]+$")
    FORMAT9 = re.compile("^[a-zA-Z0-9_]+-[a-zA-Z0-9_]+-[0-9]{6}$")
    FORMAT10 = re.compile("^[a-zA-Z0-9_]+-[a-zA-Z0-9_]+-SWAP$")

    def __init__(
        self,
        exchanges_api_keys: dict[str, dict[str, Any]],
        parsers: Optional[dict[str, Type[Parser]]] = None,
        fee_models: Optional[FeeModel | dict[Asset, FeeModel]] = None,
    ):
        self.exchanges_api_keys = exchanges_api_keys
        self.exchanges = list(exchanges_api_keys.keys())
        self.authorized_exchanges = ccxt.exchanges
        self.parsers = parsers
        self.fee_models = FeeModelManager(fee_models)
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

    def get_fee_model(self, asset: Asset) -> Optional[FeeModel]:
        if asset not in self.fee_models:
            fee_models = self.fetch_fees(asset.exchange)
            self.fee_models.multi_update(fee_models)
        return self.fee_models[asset]

    def format_symbol(self, exchange: str, symbol: str) -> str:
        return CcxtConnector.FORMAT_FUNC[exchange](symbol)

    def fetch_fees(self, exchange: str) -> Optional[dict[Asset, FeeModel]]:
        # Get fees
        exchange_to_lower = exchange.lower()
        exchange_instance = self.get_exchange_instance(exchange_to_lower)

        # first check if we can retrieve historical data
        if (
                "fetchOHLCV" not in exchange_instance.has
                or not exchange_instance.has["fetchOHLCV"]
        ):
            LOG.info(f"ccxt doesn't have fetchOHLCV function for %s", exchange)
            return None
        if (
                "fetchMarkets" not in exchange_instance.has
                or not exchange_instance.has["fetchMarkets"]
        ):
            LOG.info("ccxt doesn't have fetchMarkets function for %s", exchange)
            return None
        try:
            market_info = exchange_instance.fetchMarkets()
        except Exception as e:
            LOG.error(str(e))
            return None

        fee_models = {}
        for symbol_info in market_info:
            symbol = symbol_info["symbol"]
            if CcxtConnector.FORMAT1.match(symbol) is not None:
                symbol = symbol.replace("/", "").upper()
            elif CcxtConnector.FORMAT2.match(symbol) is not None:
                symbol = symbol.replace(".", "").upper()
            elif CcxtConnector.FORMAT3.match(symbol) is not None:
                symbol = symbol.replace("$", "").upper()
            elif CcxtConnector.FORMAT4.match(symbol) is not None:
                symbol = symbol.replace("-", "").upper()
            elif CcxtConnector.FORMAT5.match(symbol) is not None:
                symbol = symbol[:-2].upper()
            elif CcxtConnector.FORMAT6.match(symbol) is not None:
                symbol = symbol[:-4].upper()
            elif CcxtConnector.FORMAT7.match(symbol) is not None:
                symbol = symbol[:-7].upper()
            elif CcxtConnector.FORMAT8.match(symbol) is not None:
                symbol = symbol[:4].upper()
            elif CcxtConnector.FORMAT9.match(symbol) is not None:
                symbol = symbol.replace("-", "")[:-6].upper()
            elif CcxtConnector.FORMAT10.match(symbol) is not None:
                symbol = symbol.replace("-", "")[:-4].upper()
            else:
                continue

            # to simplify we just take the maximum of the 2 fees
            if "maker" not in symbol_info and "taker" not in symbol_info:
                continue
            maker_fee = taker_fee = 0
            if "maker" in symbol_info and symbol_info["maker"] is not None:
                maker_fee = float(symbol_info["maker"])
            if "taker" in symbol_info and symbol_info["taker"] is not None:
                taker_fee = float(symbol_info["taker"])
            fee = max(maker_fee, taker_fee)
            fee_models[Asset(symbol, exchange)] = PercentFeeModel(commission_pct=fee)

        return fee_models


