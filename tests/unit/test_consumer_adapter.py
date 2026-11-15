"""The pika adapter maps handler decisions onto ack / reject / nack."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from control_plane.messaging.consumer import Disposition, TaskUpdateConsumer


def _deliver(disposition: Disposition, headers: dict[str, int] | None = None) -> MagicMock:
    handler = MagicMock()
    handler.handle.return_value = disposition
    consumer = TaskUpdateConsumer("amqp://unused", handler)
    channel, connection = MagicMock(), MagicMock()
    consumer.on_message(
        connection,
        channel,
        SimpleNamespace(delivery_tag=7),
        SimpleNamespace(headers=headers),
        b"{}",
    )
    handler.handle.assert_called_once_with(b"{}")
    channel.connection_for_test = connection
    return channel


def test_ack() -> None:
    channel = _deliver(Disposition.ACK)
    channel.basic_ack.assert_called_once_with(7)
    channel.basic_reject.assert_not_called()
    channel.basic_nack.assert_not_called()


def test_poison_message_is_dead_lettered_not_requeued() -> None:
    channel = _deliver(Disposition.DEAD_LETTER)
    channel.basic_reject.assert_called_once_with(7, requeue=False)
    channel.basic_ack.assert_not_called()


@pytest.mark.parametrize("headers", [None, {"x-delivery-count": 3}])
def test_transient_failure_backs_off_then_requeues(headers: dict[str, int] | None) -> None:
    channel = _deliver(Disposition.RETRY, headers)
    channel.connection_for_test.sleep.assert_called_once()
    (delay,) = channel.connection_for_test.sleep.call_args.args
    assert 0 < delay <= 10
    channel.basic_nack.assert_called_once_with(7, requeue=True)
    channel.basic_ack.assert_not_called()
