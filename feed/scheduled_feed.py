from apscheduler.schedulers.blocking import BlockingScheduler
from feed.feed import get_alpha_vantage, insert_candles_to_db, get_latest_candle_json, push_latest_candle_to_rabbit
import settings
import logger
import os

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, 'output.log'))

sched = BlockingScheduler()


@sched.scheduled_job('interval', seconds=30)
def timed_job():
    candles = get_alpha_vantage()
    new_candles = insert_candles_to_db(candles)
    if len(new_candles) > 0:
        json_latest_candle = get_latest_candle_json(new_candles)
        push_latest_candle_to_rabbit(json_latest_candle)
    else:
        LOG.info("No new candle")

sched.start()
