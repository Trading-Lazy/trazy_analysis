from bot.data_consumer import DataConsumer
from feed.feed import Feed


class DataFlow:
    def __init__(self, feed: Feed, data_consumer: DataConsumer):
        self.feed = feed
        self.data_consumer = data_consumer

    def start(self):
        self.data_consumer.start()
        self.feed.start()
