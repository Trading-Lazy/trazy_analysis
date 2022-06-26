import sys

sys.path.append("/Users/moussadiakite/Google_Drive/Projects/MInvest/Trazy/")

from trazy_analysis.broker.percent_fee_model import PercentFeeModel
from collections import deque
from trazy_analysis.bot.event_loop import EventLoop
from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.broker.simulated_broker import SimulatedBroker
from trazy_analysis.common.clock import SimulatedClock
from trazy_analysis.feed.feed import ExternalStorageFeed, Feed
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction, OrderType
from trazy_analysis.models.order import Order
from trazy_analysis.order_manager.order_creator import OrderCreator
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.order_manager.position_sizer import PositionSizer
from trazy_analysis.strategy.strategies.arbitrage_strategy import ArbitrageStrategy
from trazy_analysis.common.helper import get_or_create_nested_dict

from trazy_analysis.models.asset import Asset
import re

from trazy_analysis.common.ccxt_connector import CcxtConnector

from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from trazy_analysis.db_storage.db_storage import DbStorage

from trazy_analysis.db_storage.mongodb_storage import MongoDbStorage
from trazy_analysis.db_storage.influxdb_storage import InfluxDbStorage
import ccxt
import logging
import numpy as np
import pandas as pd
import pytz
from trazy_analysis.market_data.historical.ccxt_historical_data_handler import (
    CcxtHistoricalDataHandler,
)
import telegram_send

LOG = logging.getLogger("airflow.task")

LOOKBACK_PERIOD = timedelta(days=1)

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

mongo_db_storage = MongoDbStorage()
influx_db_storage = InfluxDbStorage()

initial_budget = 2000
empty_result = {
    "profit_pct": 0,
    "cash_profit_pct": 0,
    "coefficient of varition (abs)": 1000000,
    "nb_errors": 0,
    "errors_pct": 0,
    "minimum number of transactions in both exchanges": 0,
    "avg_volume_in_cash": 0,
    "median_volume_in_cash": 0,
}


def ccxt_exchanges_list(db_storage: DbStorage):
    exchanges = ccxt.exchanges
    db_storage.save_state(state_key="ccxt_avalaible_exchanges", content=exchanges)
    Variable.set("ccxt_exchanges", exchanges, serialize_json=True)


def ccxt_exchange_fees(**kwargs):
    exchange = kwargs["exchange"]
    ccxt_connector = kwargs["ccxt_connector"]

    # Get fees

    # Tickers format
    format1 = re.compile("^[a-zA-Z0-9_]+/[a-zA-Z0-9_]+$")
    format2 = re.compile("^\.[a-zA-Z0-9_]+$")
    format3 = re.compile("^\$[a-zA-Z0-9_]+/[a-zA-Z0-9_]+$")
    format4 = re.compile("^[a-zA-Z0-9_]+-[a-zA-Z0-9_]+$")
    format5 = re.compile("^[a-zA-Z0-9_]+FP$")
    format6 = re.compile("^[a-zA-Z0-9_]+_BQX$")
    format7 = re.compile("^[a-zA-Z0-9_]+_[0-9]{6}$")
    format8 = re.compile("^CMT_[a-zA-Z0-9_]+$")
    format9 = re.compile("^[a-zA-Z0-9_]+-[a-zA-Z0-9_]+-[0-9]{6}$")
    format10 = re.compile("^[a-zA-Z0-9_]+-[a-zA-Z0-9_]+-SWAP$")

    LOG.info(f"Building fee models for exchange {exchange}")
    exchange_to_lower = exchange.lower()
    exchange_instance = ccxt_connector.get_exchange_instance(exchange_to_lower)

    # first check if we can retrieve historical data
    if (
        "fetchOHLCV" not in exchange_instance.has
        or not exchange_instance.has["fetchOHLCV"]
    ):
        LOG.info(f"ccxt doesn't have fetchOHLCV function for {exchange}")
        return
    if (
        "fetchMarkets" not in exchange_instance.has
        or not exchange_instance.has["fetchMarkets"]
    ):
        LOG.info(f"ccxt doesn't have fetchMarkets function for {exchange}")
        return
    try:
        market_info = exchange_instance.fetchMarkets()
    except Exception as e:
        LOG.error(str(e))
        return

    exchange_fees = {}
    symbol_mapping = {}
    for symbol_info in market_info:
        symbol = symbol_info["symbol"]
        symbol_before = symbol
        if format1.match(symbol) is not None:
            symbol = symbol.replace("/", "").upper()
        elif format2.match(symbol) is not None:
            symbol = symbol.replace(".", "").upper()
        elif format3.match(symbol) is not None:
            symbol = symbol.replace("$", "").upper()
        elif format4.match(symbol) is not None:
            symbol = symbol.replace("-", "").upper()
        elif format5.match(symbol) is not None:
            symbol = symbol[:-2].upper()
        elif format6.match(symbol) is not None:
            symbol = symbol[:-4].upper()
        elif format7.match(symbol) is not None:
            symbol = symbol[:-7].upper()
        elif format8.match(symbol) is not None:
            symbol = symbol[:4].upper()
        elif format9.match(symbol) is not None:
            symbol = symbol.replace("-", "")[:-6].upper()
        elif format10.match(symbol) is not None:
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
        exchange_fees[symbol] = {"maker": maker_fee, "taker": taker_fee}
        symbol_mapping[symbol] = symbol_before

    mongo_db_storage.save_state(state_key=f"{exchange}_fees", content=exchange_fees)
    mongo_db_storage.save_state(
        state_key=f"{exchange}_symbol_mapping", content=symbol_mapping
    )


def ccxt_exchange_pairs(**kwargs):
    ti = kwargs["ti"]
    exchanges = kwargs["exchanges"]
    db_storage = kwargs["db_storage"]

    # build exchange pairs
    seen = set()
    exchange_pairs = []
    filtered_exchanges_set = set()
    for exchange1 in exchanges:
        LOG.info(f"exchange = {exchange1}")
        if db_storage.get_state(f"{exchange1}_symbol_mapping") is None:
            seen.add(exchange1)
            continue
        for exchange2 in exchanges:
            if exchange2 in seen or exchange1 == exchange2:
                continue
            if db_storage.get_state(f"{exchange2}_symbol_mapping") is None:
                seen.add(exchange2)
                continue
            exchange_pairs.append((exchange1, exchange2))
            filtered_exchanges_set.add(exchange1)
            filtered_exchanges_set.add(exchange2)
        seen.add(exchange1)

    ti.xcom_push(key="ccxt_filtered_exchanges", value=list(filtered_exchanges_set))
    ti.xcom_push(key="ccxt_exchange_pairs", value=exchange_pairs)


def ccxt_common_pairs(**kwargs):
    ti = kwargs["ti"]
    db_storage = kwargs["db_storage"]

    filtered_exchanges = ti.xcom_pull(
        key="ccxt_filtered_exchanges", task_ids=["ccxt_build_exchange_pairs"]
    )[0]
    exchange_usable_pairs = {}
    exchanges_symbol_mapping = {}
    for exchange in filtered_exchanges:
        exchange_usable_pairs[exchange] = set()
        symbol_mapping = db_storage.get_state(f"{exchange}_symbol_mapping")
        exchanges_symbol_mapping[exchange] = symbol_mapping

    exchange_pairs = ti.xcom_pull(
        key="ccxt_exchange_pairs", task_ids=["ccxt_build_exchange_pairs"]
    )[0]
    common_pairs_dict = {}
    for exchange_pair in exchange_pairs:
        exchange1 = exchange_pair[0]
        exchange2 = exchange_pair[1]
        get_or_create_nested_dict(common_pairs_dict, exchange1)

        exchange1_tickers_list = list(exchanges_symbol_mapping[exchange1].keys())
        exchange2_tickers_list = list(exchanges_symbol_mapping[exchange2].keys())

        common_pairs = np.intersect1d(exchange1_tickers_list, exchange2_tickers_list)

        def ends_with_stable_coin(pair: str):
            stable_coins = [
                "USDT",
                "USDC",
                "BUSD",
                "DAI",
                "UST",
                "TUSD",
                "PAX",
                "HUSD",
                "USDN",
                "GUSD",
            ]
            for stable_coin in stable_coins:
                if pair.endswith(stable_coin):
                    return True
            return False

        # Consider only stable coin pairs for now
        common_pairs = [
            common_pair
            for common_pair in common_pairs
            if ends_with_stable_coin(common_pair)
        ]
        if len(common_pairs) == 0:
            continue

        exchange_usable_pairs[exchange1] |= set(common_pairs)
        exchange_usable_pairs[exchange2] |= set(common_pairs)

        common_pairs_dict[exchange1][exchange2] = common_pairs

    for exchange in exchange_usable_pairs.keys():
        exchange_usable_pairs[exchange] = list(exchange_usable_pairs[exchange])

    db_storage.save_state(state_key="ccxt_common_pairs", content=common_pairs_dict)
    ti.xcom_push(key="exchange_usable_pairs", value=exchange_usable_pairs)
    ti.xcom_push(
        key="ccxt_filtered_exchanges", value=list(exchange_usable_pairs.keys())
    )


def ccxt_historical_data_combinations(**kwargs):
    ti = kwargs["ti"]
    filtered_exchanges = ti.xcom_pull(
        key="ccxt_filtered_exchanges", task_ids=["ccxt_build_common_pairs"]
    )[0]
    exchange_usable_pairs = ti.xcom_pull(
        key="exchange_usable_pairs", task_ids=["ccxt_build_common_pairs"]
    )[0]
    historical_data_combinations = []
    for exchange in filtered_exchanges:
        all_pairs = exchange_usable_pairs[exchange]
        for pair in all_pairs:
            historical_data_combinations.append((exchange, pair))
    Variable.set(
        key="ccxt_historical_data_combinations",
        value=historical_data_combinations,
        serialize_json=True,
    )


def ccxt_historical_data(**kwargs):
    combination = kwargs["combination"]
    state_db_storage = kwargs["state_db_storage"]
    output_db_storage = kwargs["output_db_storage"]
    start = kwargs["start"]
    end = kwargs["end"]
    historical_data_handler = kwargs["historical_data_handler"]
    exchange, pair = combination[0], combination[1]
    symbol_mapping = state_db_storage.get_state(state_key=f"{exchange}_symbol_mapping")
    LOG.info(f"Downloading {exchange}-{pair}")
    original_pair = symbol_mapping[pair]
    exchange_asset = Asset(symbol=original_pair, exchange=exchange)
    historical_data_handler.save_ticker_data_in_db_storage(
        exchange_asset, output_db_storage, start, end
    )
    LOG.info(f"Finished downloading {exchange}-{pair}")


def ccxt_arbitrage_strategy_combinations(**kwargs):
    ti = kwargs["ti"]
    db_storage = kwargs["db_storage"]
    arbitrage_strategy_combinations = []
    exchange_pairs = ti.xcom_pull(
        key="ccxt_exchange_pairs", task_ids=["ccxt_build_exchange_pairs"]
    )[0]
    common_pairs_dict = db_storage.get_state(state_key="ccxt_common_pairs")
    for exchange_pair in exchange_pairs:
        exchange1 = exchange_pair[0]
        exchange2 = exchange_pair[1]

        common_pairs = common_pairs_dict[exchange1][exchange2]
        for common_pair in common_pairs:
            arbitrage_strategy_combinations.append((exchange1, exchange2, common_pair))
    Variable.set(
        key="ccxt_arbitrage_strategy_combinations",
        value=arbitrage_strategy_combinations,
        serialize_json=True,
    )


def ccxt_arbitrage_strategy(**kwargs):
    state_db_storage = kwargs["state_db_storage"]
    external_db_storage = kwargs["external_db_storage"]
    combination = kwargs["combination"]
    start = kwargs["start"]
    end = kwargs["end"]

    exchange1, exchange2, common_pair = combination[0], combination[1], combination[2]
    exchanges = [exchange1, exchange2]
    common_pair_key = f"{exchange1}_{exchange2}_{common_pair}"
    LOG.info(f"Checking arbitrage opportunities for {common_pair}")
    events = deque()
    exchange1_symbol_mapping = state_db_storage.get_state(
        state_key=f"{exchange1}_symbol_mapping"
    )
    exchange2_symbol_mapping = state_db_storage.get_state(
        state_key=f"{exchange2}_symbol_mapping"
    )
    common_pair1 = exchange1_symbol_mapping[common_pair]
    common_pair2 = exchange2_symbol_mapping[common_pair]
    assets = [
        Asset(symbol=common_pair1, exchange=exchange1),
        Asset(symbol=common_pair2, exchange=exchange2),
    ]
    assets_dict = {asset.exchange: asset for asset in assets}

    feed: Feed = ExternalStorageFeed(assets=assets, start=start, end=end, events=events, db_storage=external_db_storage,
                                     file_storage=None, market_cal=None)

    # Check whether data is empty or not
    exchange1_candle_dataframe = feed.candle_dataframes[assets_dict[exchange1]][timedelta(minutes=1)]

    state_key = f"{common_pair_key}_arbitrage_result"
    if exchange1_candle_dataframe.empty:
        LOG.info(
            f"{common_pair_key} data is empty for exchange1_candle_dataframe so it is skipped."
        )
        state_db_storage.save_state(state_key=state_key, content=empty_result)
        return

    exchange2_candle_dataframe = feed.candle_dataframes[assets_dict[exchange2]][timedelta(minutes=1)]

    if exchange2_candle_dataframe.empty:
        LOG.info(
            f"{common_pair_key} data is empty for exchange2_candle_dataframe so it is skipped."
        )
        state_db_storage.save_state(state_key=state_key, content=empty_result)
        return

    # don't process the pair if the volume is low
    exchange1_prices = pd.to_numeric(exchange1_candle_dataframe["close"])
    exchange1_volumes = pd.to_numeric(exchange1_candle_dataframe["volume"])
    exchange1_avg_price = exchange1_prices.mean()
    exchange1_median_price = exchange1_prices.median()
    exchange1_avg_volume = exchange1_volumes.mean()
    exchange1_median_volume = exchange1_volumes.median()

    exchange2_prices = pd.to_numeric(exchange2_candle_dataframe["close"])
    exchange2_volumes = pd.to_numeric(exchange2_candle_dataframe["volume"])
    exchange2_avg_price = exchange2_prices.mean()
    exchange2_median_price = exchange2_prices.median()
    exchange2_avg_volume = exchange2_volumes.mean()
    exchange2_median_volume = exchange2_volumes.median()

    avg_price = min(exchange1_avg_price, exchange2_avg_price)
    median_price = min(exchange1_median_price, exchange2_median_price)
    avg_volume = min(exchange1_avg_volume, exchange2_avg_volume)
    median_volume = min(exchange1_median_volume, exchange2_median_volume)

    avg_volume_in_cash = avg_volume * avg_price
    median_volume_in_cash = median_volume * median_price

    # if volume_in_cash == 0.0:
    #    LOG.info(f"{common_pair_key} volume in cash {volume_in_cash} is lower than the minimum volume in cash required {MINIMUM_VOLUME_IN_CASH} so it is skipped.")
    #    LOG.info(f"Current rank: {profit_rank}")
    #    continue

    # Create brokers for exchanges, put a big amount of cash and a big amount of shares to allow all two
    # ways transactions
    strategies = {ArbitrageStrategy: {"margin_factor": 0.5}}

    clock = SimulatedClock()
    initial_funds = initial_budget / 2

    exchange1_fee_model_dict = state_db_storage.get_state(
        state_key=f"{exchange1}_fees"
    )[common_pair]
    if (
        "maker" in exchange1_fee_model_dict
        and exchange1_fee_model_dict["maker"] is not None
    ):
        exchange1_maker_fee = float(exchange1_fee_model_dict["maker"])
    if (
        "taker" in exchange1_fee_model_dict
        and exchange1_fee_model_dict["taker"] is not None
    ):
        exchange1_taker_fee = float(exchange1_fee_model_dict["taker"])
    exchange1_fee = max(exchange1_maker_fee, exchange1_taker_fee)
    exchange1_fee_model = PercentFeeModel(commission_pct=exchange1_fee)

    exchange2_fee_model_dict = state_db_storage.get_state(
        state_key=f"{exchange2}_fees"
    )[common_pair]
    if (
        "maker" in exchange2_fee_model_dict
        and exchange2_fee_model_dict["maker"] is not None
    ):
        exchange2_maker_fee = float(exchange2_fee_model_dict["maker"])
    if (
        "taker" in exchange2_fee_model_dict
        and exchange2_fee_model_dict["taker"] is not None
    ):
        exchange2_taker_fee = float(exchange2_fee_model_dict["taker"])
    exchange2_fee = max(exchange2_maker_fee, exchange2_taker_fee)
    exchange2_fee_model = PercentFeeModel(commission_pct=exchange2_fee)

    # LOG.info(f"initial_funds = {initial_funds}")
    # Create brokers with a big amount of money
    exchange1_broker = SimulatedBroker(
        clock,
        events,
        initial_funds=initial_funds,
        fee_model=exchange1_fee_model,
        exchange=exchange1,
    )
    exchange1_broker.subscribe_funds_to_portfolio(initial_funds)
    exchange1_first_candle = exchange1_candle_dataframe.get_candle(0)
    exchange1_broker.update_price(exchange1_first_candle)

    exchange2_broker = SimulatedBroker(
        clock,
        events,
        initial_funds=initial_funds,
        fee_model=exchange2_fee_model,
        exchange=exchange2,
    )
    # exchange2_broker.subscribe_funds_to_portfolio(initial_funds)
    exchange2_first_candle = exchange2_candle_dataframe.get_candle(0)
    exchange2_broker.update_price(exchange2_first_candle)
    max_size_exchange1 = exchange1_broker.max_entry_order_size(
        assets_dict[exchange1], Direction.LONG, initial_funds
    )
    max_size_exchange2 = exchange2_broker.max_entry_order_size(
        assets_dict[exchange2], Direction.LONG, initial_funds
    )
    initial_size = max_size_exchange2

    # exchange 2
    candle = Candle(asset=assets_dict[exchange2], open=0, high=0, low=0, close=0, volume=0)
    exchange2_broker.update_price(candle)
    order = Order(
        asset=assets_dict[exchange2],
        action=Action.BUY,
        direction=Direction.LONG,
        size=initial_size,
        signal_id="0",
        limit=None,
        stop=None,
        target=None,
        stop_pct=None,
        type=OrderType.MARKET,
        clock=clock,
        time_in_force=timedelta(minutes=5),
    )
    exchange2_broker.execute_market_order(order)

    # prepare event loop parameters
    broker_manager = BrokerManager(brokers_per_exchange={
        exchange1: exchange1_broker,
        exchange2: exchange2_broker,
    })
    position_sizer = PositionSizer(broker_manager=broker_manager, integer_size=False)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(events=events, broker_manager=broker_manager, position_sizer=position_sizer,
                                 order_creator=order_creator, clock=clock)
    indicators_manager = IndicatorsManager(preload=True, initial_data=feed.candles)
    event_loop = EventLoop(events=events, assets=assets, feed=feed, order_manager=order_manager,
                           indicators_manager=indicators_manager, strategies_parameters=strategies,
                           close_at_end_of_day=False, close_at_end_of_data=False)

    # get initial state of portfolio for stats computation total_market_value
    exchange1_broker.update_price(exchange1_first_candle)
    exchange2_broker.update_price(exchange2_first_candle)
    initial_market_values = sum(
        [
            broker_manager.get_broker(exchange).get_portfolio_total_market_value()
            for exchange in exchanges
        ]
    )
    initial_cash_balances = sum(
        [
            broker_manager.get_broker(exchange).get_portfolio_cash_balance()
            for exchange in exchanges
        ]
    )
    initial_equities = initial_cash_balances + initial_market_values

    event_loop.loop()

    exchange1_history = exchange1_broker.portfolio.history
    exchange1_transactions = [
        portfolio_event
        for portfolio_event in exchange1_history
        if portfolio_event.event_type == "symbol_transaction"
    ]
    # remove first transaction, which is to add securities in the broker
    exchange1_transactions = exchange1_transactions[1:]

    exchange2_history = exchange2_broker.portfolio.history
    exchange2_transactions = [
        portfolio_event
        for portfolio_event in exchange2_history
        if portfolio_event.event_type == "symbol_transaction"
    ]
    # remove first transaction, which is to add securities in the broker
    exchange2_transactions = exchange2_transactions[1:]

    nb_transactions1 = len(exchange1_transactions)
    nb_transactions2 = len(exchange2_transactions)
    min_nb_transactions = min(nb_transactions1, nb_transactions2)
    # if min_nb_transactions < MINIMUM_TRANSACTIONS:
    #    LOG.info(f"{common_pair_key} has only {min_nb_transactions} transactions which is less than the minimum required number of transactions {MINIMUM_TRANSACTIONS} so it is skipped.")
    #    LOG.info(f"Current rank: {profit_rank}")
    #    continue

    # find missed opportunities
    """
    When an arbitrage opportunity is found, we submit 2 orders to the brokers of the 2 exchanges.
    If one of the order is not executed because of for example not enough cash or, whathever reason,
    we call it an "error". The less errors you have, the better it is for ensuring the stability of the strategy
    """
    nb_errors = 0
    transaction_profits = []
    i = 0
    j = 0

    while i < nb_transactions1 and j < nb_transactions2:
        transaction1 = exchange1_transactions[i]
        transaction2 = exchange2_transactions[j]
        if transaction1.timestamp == transaction2.timestamp:
            i += 1
            j += 1
        elif transaction1.timestamp < transaction2.timestamp:
            i += 1
            nb_errors += 1
        else:  # transaction1.timestamp > transaction2.timestamp
            j += 1
            nb_errors += 1
        if transaction1.action == Action.BUY:
            transaction_profit = transaction2.credit - transaction1.debit
        else:
            transaction_profit = transaction1.credit - transaction2.debit
        transaction_profits.append(transaction_profit)

    # if nb_errors > min_nb_transactions:
    #    LOG.info(f"{common_pair_key} nb errors is greater than number of transactions {min_nb_transactions} transactions so it is skipped.")
    #    continue

    volume_in_cash_result = {
        "profit_pct": 0,
        "cash_profit_pct": 0,
        "coefficient of varition (abs)": 1000000,
        "nb_errors": 0,
        "errors_pct": 0,
        "minimum number of transactions in both exchanges": min_nb_transactions,
        "avg_volume_in_cash": avg_volume_in_cash,
        "median_volume_in_cash": median_volume_in_cash,
    }
    if len(transaction_profits) == 0:
        LOG.info(f"{common_pair_key} transaction_profits is zero so it is skipped.")
        state_db_storage.save_state(state_key=state_key, content=volume_in_cash_result)
        return

    errors_pct = 0
    max_nb_transactions = max(nb_transactions1, nb_transactions2)
    if max_nb_transactions != 0:
        errors_pct = nb_errors / max_nb_transactions * 100
    cash_profit = sum(transaction_profits)

    # let's find the coefficient of variation to filter
    cv = lambda x: np.std(x, ddof=1) / np.mean(x) * 100
    coefficient_of_variation = cv(transaction_profits)

    final_market_values = sum(
        [
            broker_manager.get_broker(exchange).get_portfolio_total_market_value()
            for exchange in exchanges
        ]
    )
    final_cash_balances = sum(
        [
            broker_manager.get_broker(exchange).get_portfolio_cash_balance()
            for exchange in exchanges
        ]
    )
    final_equities = final_market_values + final_cash_balances

    profit = final_equities - initial_equities

    profit_pct = profit / initial_equities * 100

    cash_profit_pct = cash_profit / initial_cash_balances * 100

    result = {
        "profit_pct": profit_pct,
        "cash_profit_pct": cash_profit_pct,
        "coefficient of varition (abs)": abs(coefficient_of_variation),
        "nb_errors": nb_errors,
        "errors_pct": errors_pct,
        "minimum number of transactions in both exchanges": min_nb_transactions,
        "avg_volume_in_cash": avg_volume_in_cash,
        "median_volume_in_cash": median_volume_in_cash,
    }
    state_db_storage.save_state(state_key=state_key, content=result)


def ccxt_arbitrage_strategy_results(**kwargs):
    db_storage = kwargs["db_storage"]

    arbitrage_strategy_combinations = Variable.get(
        "ccxt_arbitrage_strategy_combinations", deserialize_json=True
    )

    results_dict = {}
    for combination in arbitrage_strategy_combinations:
        exchange1, exchange2, common_pair = (
            combination[0],
            combination[1],
            combination[2],
        )
        result_key = f"{exchange1}_{exchange2}_{common_pair}"
        state_key = f"{result_key}_arbitrage_result"
        result = db_storage.get_state(state_key=state_key)
        if result is not None:
            results_dict[result_key] = result

    results_df = pd.DataFrame.from_dict(results_dict, orient="index")
    results_df.index.name = "Exchange and crypto pairs"

    # rank results
    # ranking results

    results_dict = results_df.to_dict(orient="index")

    db_storage.save_state(
        state_key="ccxt_arbitrage_strategy_results", content=results_dict
    )

    if len(results_df) != 0:
        best_result_series = results_df.iloc[0]
        best_results_dict = best_result_series.to_dict()
        best_results_dict["exchange_pairs_crypto_pairs"] = best_result_series.name
        db_storage.save_state(
            state_key="ccxt_best_arbitrage_strategy_result", content=best_results_dict
        )


def ccxt_results_to_telegram(**kwargs):
    db_storage = kwargs["db_storage"]
    best_result_dict = db_storage.get_state(
        state_key="ccxt_best_arbitrage_strategy_result"
    )
    telegram_send.send(
        messages=[
            "The best result for the ccxt arbitrage strategy is:",
            "\n".join([f"{key}: {value}" for key, value in best_result_dict.items()]),
        ]
    )


def clean_everything(**kwargs):
    db_storage = kwargs["db_storage"]
    db_storage.clean_all_states()
    db_storage.clean_all_candles()


with DAG(
    "ccxt_market_data_pipeline",
    start_date=datetime(2021, 8, 8),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    get_ccxt_exchanges_list = PythonOperator(
        task_id="get_ccxt_exchanges_list",
        python_callable=ccxt_exchanges_list,
        op_args=[mongo_db_storage],
        retries=3,
    )

    try:
        exchanges = Variable.get("ccxt_exchanges", deserialize_json=True)
    except Exception as e:
        exchanges = []
    if len(exchanges) == 0:
        get_ccxt_exchanges_list
    else:
        exchanges_api_keys = {
            exchange: {
                "key": None,
                "secret": None,
                "password": None,
            }
            for exchange in exchanges
        }
        ccxt_connector = CcxtConnector(exchanges_api_keys=exchanges_api_keys)
        ccxt_exchange_fees_tasks = [
            PythonOperator(
                task_id=f"get_{exchange.lower()}_fees",
                python_callable=ccxt_exchange_fees,
                op_kwargs={"exchange": exchange, "ccxt_connector": ccxt_connector},
                retries=3,
            )
            for exchange in exchanges
        ]

        ccxt_build_exchange_pairs = PythonOperator(
            task_id="ccxt_build_exchange_pairs",
            python_callable=ccxt_exchange_pairs,
            op_kwargs={"exchanges": exchanges, "db_storage": mongo_db_storage},
        )

        ccxt_build_common_pairs = PythonOperator(
            task_id="ccxt_build_common_pairs",
            python_callable=ccxt_common_pairs,
            op_kwargs={"db_storage": mongo_db_storage},
        )

        ccxt_build_historical_data_combinations = PythonOperator(
            task_id="ccxt_build_historical_data_combinations",
            python_callable=ccxt_historical_data_combinations,
            op_kwargs={"db_storage": mongo_db_storage},
        )

        clean_everything = PythonOperator(
            task_id="clean_everything",
            python_callable=clean_everything,
            op_kwargs={"db_storage": mongo_db_storage},
        )

        try:
            historical_data_combinations = Variable.get(
                "ccxt_historical_data_combinations", deserialize_json=True
            )
        except Exception as e:
            historical_data_combinations = []
        if len(historical_data_combinations) == 0:
            (
                get_ccxt_exchanges_list
                >> ccxt_exchange_fees_tasks
                >> ccxt_build_exchange_pairs
                >> ccxt_build_common_pairs
            )
        else:
            end = datetime(2021, 8, 8, 19, 7, 4, 190316, tzinfo=pytz.UTC)
            start = datetime(2021, 8, 7, 19, 7, 4, 190316, tzinfo=pytz.UTC)
            historical_data_handler = CcxtHistoricalDataHandler(ccxt_connector)
            ccxt_download_historical_data_combinations_tasks = [
                PythonOperator(
                    task_id=f"ccxt_{combination[0].lower()}_{combination[1].lower()}_download_historical_data",
                    python_callable=ccxt_historical_data,
                    op_kwargs={
                        "combination": combination,
                        "state_db_storage": mongo_db_storage,
                        "output_db_storage": influx_db_storage,
                        "start": start,
                        "end": end,
                        "historical_data_handler": historical_data_handler,
                    },
                )
                for combination in historical_data_combinations
            ]

            ccxt_build_arbitrage_strategy_combinations = PythonOperator(
                task_id="ccxt_build_arbitrage_strategy_combinations",
                python_callable=ccxt_arbitrage_strategy_combinations,
                op_kwargs={"db_storage": mongo_db_storage},
            )

            try:
                ccxt_arbitrage_strategy_combinations = Variable.get(
                    "ccxt_arbitrage_strategy_combinations", deserialize_json=True
                )
            except Exception as e:
                ccxt_arbitrage_strategy_combinations = []
            if len(ccxt_arbitrage_strategy_combinations) == 0:
                (
                    get_ccxt_exchanges_list
                    >> ccxt_exchange_fees_tasks
                    >> ccxt_build_exchange_pairs
                    >> ccxt_build_common_pairs
                    >> ccxt_build_historical_data_combinations
                    >> ccxt_download_historical_data_combinations_tasks
                    >> ccxt_build_arbitrage_strategy_combinations
                )
            else:
                ccxt_arbitrage_strategy_tasks = [
                    PythonOperator(
                        task_id=f"ccxt_{combination[0]}_{combination[1]}_{combination[2]}_process_arbitrage_strategy",
                        python_callable=ccxt_arbitrage_strategy,
                        op_kwargs={
                            "combination": combination,
                            "state_db_storage": mongo_db_storage,
                            "external_db_storage": influx_db_storage,
                            "start": start,
                            "end": end,
                        },
                    )
                    for combination in ccxt_arbitrage_strategy_combinations
                ]

                ccxt_rank_arbitrage_strategy_results = PythonOperator(
                    task_id="ccxt_rank_arbitrage_strategy_results",
                    python_callable=ccxt_arbitrage_strategy_results,
                    op_kwargs={"db_storage": mongo_db_storage},
                )

                ccxt_send_results_to_telegram = PythonOperator(
                    task_id="ccxt_send_results_to_telegram",
                    python_callable=ccxt_results_to_telegram,
                    op_kwargs={"db_storage": mongo_db_storage},
                )

                (
                    get_ccxt_exchanges_list
                    >> ccxt_exchange_fees_tasks
                    >> ccxt_build_exchange_pairs
                    >> ccxt_build_common_pairs
                    >> ccxt_build_historical_data_combinations
                    >> ccxt_download_historical_data_combinations_tasks
                    >> ccxt_build_arbitrage_strategy_combinations
                    >> ccxt_arbitrage_strategy_tasks
                    >> ccxt_rank_arbitrage_strategy_results
                )
