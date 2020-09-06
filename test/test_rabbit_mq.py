import time
from unittest.mock import MagicMock, call, patch

import settings
from candles_queue.rabbit_mq import RabbitMq

QUEUE_NAME1 = "candles1"
QUEUE_NAME2 = "candles2"
QUEUE_NAME3 = "candles3"
QUEUE_NAME4 = "candles4"
MESSAGE = "Rabbits like carrots"
CONNECTION_URL = settings.CLOUDAMQP_URL


@patch("pika.BlockingConnection")
def test_push(blocking_connection_mocked):
    blocking_connection_mocked.return_value = MagicMock()
    connection_mocked_returned_value = blocking_connection_mocked.return_value
    connection_mocked_returned_value.channel = MagicMock()
    channel_mocked = connection_mocked_returned_value.channel
    channel_mocked.return_value = MagicMock()
    channel_mocked_return_value = channel_mocked.return_value
    channel_mocked_return_value.queue_declare = MagicMock()
    queue_declare_mocked = channel_mocked_return_value.queue_declare
    channel_mocked_return_value.basic_publish = MagicMock()
    basic_publish_mocked = channel_mocked_return_value.basic_publish

    rabbit_mq = RabbitMq(QUEUE_NAME1, CONNECTION_URL)

    assert rabbit_mq.connection == connection_mocked_returned_value
    assert rabbit_mq.push_channel == channel_mocked.return_value

    rabbit_mq.push(MESSAGE)

    queue_declare_calls = [call(queue=QUEUE_NAME1)]
    queue_declare_mocked.assert_has_calls(queue_declare_calls)

    basic_publish_calls = [call(exchange="", routing_key=QUEUE_NAME1, body=MESSAGE)]
    basic_publish_mocked.assert_has_calls(basic_publish_calls)


def test_add_consumer():
    received_message = None

    def callback(message: str):
        nonlocal received_message
        received_message = message

    rabbit_mq = RabbitMq(QUEUE_NAME1, CONNECTION_URL)
    rabbit_mq.add_consumer(callback)
    rabbit_mq.push(MESSAGE)

    time.sleep(2)

    assert received_message == str.encode(MESSAGE)


def test_add_consumer_with_ack():
    number_of_retries = 0
    received_messages = []

    def callback(message: str):
        nonlocal number_of_retries
        nonlocal received_messages
        number_of_retries += 1
        received_messages.append(message)
        if number_of_retries < 2:
            raise Exception()

    rabbit_mq = RabbitMq(QUEUE_NAME2, CONNECTION_URL)
    rabbit_mq.flush()
    rabbit_mq.add_consumer_with_ack(callback)
    rabbit_mq.push(MESSAGE)

    time.sleep(2)

    assert number_of_retries == 2
    for received_message in received_messages:
        assert received_message == str.encode(MESSAGE)


def test_flush():
    rabbit_mq = RabbitMq(QUEUE_NAME3, CONNECTION_URL)

    rabbit_mq.push(MESSAGE)
    rabbit_mq.push(MESSAGE)
    rabbit_mq.flush()
    time.sleep(1)
    declared_queue = rabbit_mq.push_channel.queue_declare(queue=QUEUE_NAME3)
    assert declared_queue.method.message_count == 0


def test_size():
    rabbit_mq = RabbitMq(QUEUE_NAME4, CONNECTION_URL)
    rabbit_mq.flush()

    rabbit_mq.push(MESSAGE)
    time.sleep(1)
    assert rabbit_mq.size() == 1
    rabbit_mq.push(MESSAGE)
    time.sleep(1)
    assert rabbit_mq.size() == 2
    rabbit_mq.flush()
