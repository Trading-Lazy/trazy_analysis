import pika
import settings
import logger
import os
import json
import multiprocessing as mp

from strategy.strategies.BuyAndSellLongStrategy import BuyAndSellLongStrategy
from strategy.strategies.DumbLongStrategy import DumbLongStrategy
from strategy.strategies.DumbShortStrategy import DumbShortStrategy
from strategy.strategies.SellAndBuyShortStrategy import SellAndBuyShortStrategy
from .strategy import *

list_strategies = []
for s in settings.CONFIG['strategies']:
    if s == StrategyName.SMA_CROSSOVER.name:
        list_strategies.append(SmaCrossoverStrategy())
    if s == StrategyName.DUMB_LONG_STRATEGY.name:
        list_strategies.append(DumbLongStrategy())
    if s == StrategyName.DUMB_SHORT_STRATEGY.name:
        list_strategies.append(DumbShortStrategy())
    if s == StrategyName.BUY_AND_SELL_LONG_STRATEGY.name:
        list_strategies.append(BuyAndSellLongStrategy())
    if s == StrategyName.SELL_AND_BUY_SHORT_STRATEGY.name:
        list_strategies.append(SellAndBuyShortStrategy())


LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, 'output.log'))


def run_strategy(strategy, candle):
    strategy.process_candle(candle)


def run_parallel_strategies(strategies, candle):
    pool = mp.Pool(mp.cpu_count())
    pool.starmap(run_strategy, [(strategy,candle) for strategy in strategies])
    pool.close()


def new_candle_callback(ch, method, properties, str_candle):
    LOG.info("Dequeue new candle: {}".format(str_candle))
    candle = Candle()
    candle.set_from_dict(json.loads(str_candle))
    run_parallel_strategies(list_strategies, candle)


if __name__ == "__main__":
    connection = pika.BlockingConnection(pika.URLParameters(settings.CLOUDAMQP_URL))
    channel = connection.channel()
    channel.queue_declare(queue='candles')

    channel.basic_consume(
        queue='candles', on_message_callback=new_candle_callback, auto_ack=False)

    channel.start_consuming()