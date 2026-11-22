"""Composition root: builds each control-plane process from the environment.

This is the only module that reads :func:`get_settings` to wire components
together; everything below it (API, relay, consumer) receives its dependencies.
"""

import uvicorn
from fastapi import FastAPI

from control_plane.api.app import create_app
from control_plane.core.config import get_settings
from control_plane.db.session import create_session_factory
from control_plane.messaging.consumer import TaskUpdateConsumer, TaskUpdateHandler
from control_plane.messaging.outbox_relay import OutboxRelay
from control_plane.messaging.publisher import RabbitMQPublisher


def create_api() -> FastAPI:
    """uvicorn factory: the HTTP API bound to the configured database."""
    return create_app(create_session_factory(get_settings().database_url))


def run_api() -> None:
    settings = get_settings()
    uvicorn.run(
        "control_plane.bootstrap:create_api",
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


PROCESSES = {"api": run_api, "relay": run_relay, "consumer": run_consumer}
