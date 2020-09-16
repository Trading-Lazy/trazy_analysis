from unittest.mock import call, patch

import settings
from candles_queue.simple_queue import SimpleQueue

QUEUE_NAME = "candles"
OTHER_QUEUE_NAME = "other_candles"
MESSAGE1 = "Rabbits like carrots"
MESSAGE2 = "I am the master of the world"
MESSAGE3 = "I like tests"
CONNECTION_URL = settings.CLOUDAMQP_URL


@patch("rx.subject.Subject.on_next")
def test_push(on_next_mocked):
    simple_queue = SimpleQueue(QUEUE_NAME)

    simple_queue.push(MESSAGE1)
    simple_queue.push(MESSAGE2)

    assert simple_queue.queue.pop() == MESSAGE1
    assert simple_queue.queue.pop() == MESSAGE2

    on_next_calls = [call(MESSAGE1), call(MESSAGE2)]
    on_next_mocked.assert_has_calls(on_next_calls)


def test_flush():
    simple_queue = SimpleQueue(QUEUE_NAME)

    simple_queue.push(MESSAGE1)
    simple_queue.push(MESSAGE2)
    simple_queue.flush()

    assert len(simple_queue.queue) == 0


def test_size():
    simple_queue = SimpleQueue(QUEUE_NAME)
    simple_queue.flush()
    simple_queue.push(MESSAGE1)
    simple_queue.push(MESSAGE2)

    assert simple_queue.size() == 2


def test_add_consumer():
    received_message = None

    def callback(message: str):
        nonlocal received_message
        received_message = message

    simple_queue = SimpleQueue(QUEUE_NAME)
    simple_queue.add_consumer(callback)
    simple_queue.push(MESSAGE1)

    assert received_message == MESSAGE1


def test_add_consumer_messages_already_in_queue():
    number_of_calls = 0
    received_messages = []

    def callback(message: str):
        nonlocal number_of_calls
        nonlocal received_messages
        number_of_calls += 1
        received_messages.append(message)

    simple_queue = SimpleQueue(QUEUE_NAME)
    simple_queue.push(MESSAGE1)
    simple_queue.push(MESSAGE2)

    simple_queue.add_consumer(callback)
    simple_queue.push(MESSAGE3)

    assert number_of_calls == 3
    assert received_messages[0] == MESSAGE1
    assert received_messages[1] == MESSAGE2
    assert received_messages[2] == MESSAGE3


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

    simple_queue = SimpleQueue(QUEUE_NAME)
    simple_queue.add_consumer_with_ack(callback)
    simple_queue.push(MESSAGE1)

    assert number_of_retries == 2
    for received_message in received_messages:
        assert received_message == MESSAGE1
