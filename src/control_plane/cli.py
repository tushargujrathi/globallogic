"""``control-plane`` entrypoint: ``api`` | ``relay`` | ``consumer``.

Each subcommand is one container in docker-compose, built from the same image.
"""

import argparse
import logging
import signal
from types import FrameType

import uvicorn

from control_plane.core.config import get_settings
from control_plane.core.logging import configure_logging
from control_plane.db.session import create_session_factory
from control_plane.messaging.consumer import TaskUpdateConsumer, TaskUpdateHandler
from control_plane.messaging.outbox_relay import OutboxRelay
from control_plane.messaging.publisher import RabbitMQPublisher


def _stop_on_sigterm() -> None:
    """Turn SIGTERM (docker stop) into KeyboardInterrupt for a clean shutdown."""

    def _raise(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _raise)


def run_api() -> None:
    settings = get_settings()
    uvicorn.run(
        "control_plane.api.app:create_app",
        factory=True,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )


def run_relay() -> None:
    settings = get_settings()
    publisher = RabbitMQPublisher(settings.rabbitmq_url)
    relay = OutboxRelay(
        create_session_factory(settings.database_url),
        publisher,
        batch_size=settings.outbox_batch_size,
        poll_interval_seconds=settings.outbox_poll_interval_seconds,
    )
    try:
        relay.run()
    finally:
        publisher.close()


def run_consumer() -> None:
    settings = get_settings()
    handler = TaskUpdateHandler(create_session_factory(settings.database_url))
    TaskUpdateConsumer(
        settings.rabbitmq_url, handler, prefetch_count=settings.consumer_prefetch_count
    ).run()


COMMANDS = {"api": run_api, "relay": run_relay, "consumer": run_consumer}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="control-plane", description=__doc__)
    parser.add_argument("command", choices=sorted(COMMANDS))
    args = parser.parse_args(argv)
    configure_logging(get_settings().log_level)
    _stop_on_sigterm()
    try:
        COMMANDS[args.command]()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("%s stopped", args.command)


if __name__ == "__main__":
    main()
