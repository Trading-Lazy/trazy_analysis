from apscheduler.schedulers.blocking import BlockingScheduler

from market_data.historical.start_historical_data_pipeline import (
    start_historical_data_pipeline,
)
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
    start_historical_data_pipeline()


sched.start()
