import os
import threading
import traceback

import logger
import settings
from candles_queue.candles_queue import CandlesQueue
from models.candle import Candle

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)

import pika
from typing import Callable


class RabbitMqConsumer:
    def __init__(
        self,
        queue_name: str,
        connection_url: str,
        callback: Callable[[str], None],
        auto_ack=False,
    ):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.URLParameters(connection_url))
        self.channel = self.connection.channel()
        self.callback = callback
        self.auto_ack = auto_ack

    def __consume(self):
        def on_message(channel, method_frame, header_frame, body):
            try:
                self.callback(body)
                if not self.auto_ack:
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            except Exception:
                LOG.error("Exception will reenqueue candle: %s", traceback.format_exc())
                channel.basic_reject(
                    delivery_tag=method_frame.delivery_tag, requeue=True
                )

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=on_message,
            auto_ack=self.auto_ack,
        )
        self.channel.start_consuming()

    @staticmethod
    def start_consumer(
        queue_name: str,
        connection_url: str,
        callback: Callable[[str], None],
        auto_ack=False,
    ):
        def create_consumer_and_start_consuming():
            consumer = RabbitMqConsumer(queue_name, connection_url, callback, auto_ack)
            consumer.__consume()

        consumer_thread = threading.Thread(target=create_consumer_and_start_consuming)
        consumer_thread.daemon = True
        consumer_thread.start()
        return consumer_thread


class RabbitMq(CandlesQueue):
    def __init__(self, queue_name: str, connection_url: str):
        super().__init__(queue_name)
        self.connection_url: str = connection_url
        self.connection = pika.BlockingConnection(pika.URLParameters(connection_url))
        self.push_channel = self.connection.channel()
        self.consumers_threads = []

    def add_consumer_helper(
        self, callback: Callable[[str], None], auto_ack=False
    ) -> None:
        consumer_thread = RabbitMqConsumer.start_consumer(
            self.queue_name, self.connection_url, callback, auto_ack=auto_ack
        )
        self.consumers_threads.append(consumer_thread)

    def add_consumer_no_retry(self, callback: Callable[[str], None]) -> None:
        self.add_consumer_helper(callback, auto_ack=True)

    def add_consumer(self, callback: Callable[[str], None]) -> None:
        self.add_consumer_helper(callback)

    def push(self, queue_elt: Candle) -> None:
        self.push_channel.queue_declare(queue=self.queue_name)
        self.push_channel.basic_publish(
            exchange="", routing_key=self.queue_name, body=queue_elt.to_json()
        )
        LOG.info("Sent new elt!")

    def flush(self) -> None:
        self.push_channel.queue_purge(queue=self.queue_name)

    def size(self) -> int:
        declared_queue = self.push_channel.queue_declare(queue=self.queue_name)
        return declared_queue.method.message_count
