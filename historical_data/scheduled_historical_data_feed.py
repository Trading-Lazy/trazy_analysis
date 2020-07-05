from apscheduler.schedulers.blocking import BlockingScheduler

from historical_data.historical_data_feed import historical_data_feed
from settings import (
    HISTORICAL_FEED_DAY_OF_WEEK,
    HISTORICAL_FEED_HOUR,
    HISTORICAL_FEED_MINUTE,
)

sched = BlockingScheduler()


@sched.scheduled_job(
    "cron",
    minute=HISTORICAL_FEED_MINUTE,
    day_of_week=HISTORICAL_FEED_DAY_OF_WEEK,
    hour=HISTORICAL_FEED_HOUR,
)
def scheduled_historical_data_feed():  # pragma: no cover
    historical_data_feed()


sched.start()
