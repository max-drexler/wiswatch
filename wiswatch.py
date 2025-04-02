#! /usr/bin/env python3

"""wiswatch

A CLI client to consume real-time geospatial data from the World Meteorological
Organization Information System (WIS2).
"""

from __future__ import annotations

__version__ = "0.1.0"
__author__ = "Max Drexler"
__email__ = "mndrexler@wisc.edu"

import argparse
import asyncio
import functools
import json
import logging
import sys
from ssl import create_default_context
from dataclasses import dataclass, field, fields
from urllib.parse import urlparse

import aiomqtt

LOG = logging.getLogger('wiswatch')


def port_per_transport(transport: str) -> int:
    if transport.lower() in ("tcp", "mqtt", "mqtts"):
        return 8883
    if transport.lower() in ("websockets", "websocket", "ws", "wss"):
        return 443
    raise ValueError(f"Unknown transport: {transport}")


def default_topics():
    """By default, listen for all core (free) data."""
    return ["cache/a/wis2/+/data/core/#"]


@dataclass
class WISConsumer:
    # Connection kwargs
    hostname: str = field(default="globalbroker.meteo.fr")
    topics: list[str] = field(default_factory=default_topics)
    port: int | None = field(default=None)
    username: str = field(default="everyone")
    password: str = field(default="everyone", repr=False)
    transport: str = field(default="tcp")

    # Non-connection kwargs
    include_topic: bool = field(default=False)
    reconnect_delay: float = field(default=3.0)
    reconnect_max: int = field(default=-1)

    _mqtt_client: aiomqtt.Client = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.port = port_per_transport(self.transport)
        self._mqtt_client = self._create_client()

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> WISConsumer:
        """Construct a consumer using a URI."""
        try:
            o = urlparse(uri, allow_fragments=False)
        except (TypeError, ValueError) as e:
            err_msg = f"Invalid URI: {uri}"
            LOG.critical(err_msg)
            raise ValueError(err_msg) from e

        # These attributes may raise an error on access if incorrect
        try:
            port = o.port
        except ValueError as e:
            err_msg = f"URI invalid port: {uri}"
            LOG.critical(err_msg)
            raise ValueError(err_msg) from e

        try:
            host = o.hostname
        except ValueError as e:
            err_msg = f"URI invalid hostname: {uri}"
            LOG.critical(err_msg)
            raise ValueError(err_msg) from e

        if not o.scheme or o.scheme.lower() in ("mqtt", "mqtts"):
            transport = "tcp"
        elif o.scheme.lower() in ("ws", "wss"):
            transport = "websockets"
        else:
            err_msg = f"Invalid scheme '{o.scheme}', must be 'wss' or 'mqtts'"
            LOG.critical(err_msg)
            raise ValueError(err_msg)

        conn_kwargs = {"transport": transport}
        if host is not None:
            conn_kwargs["hostname"] = host

        if o.username is not None:
            conn_kwargs["username"] = o.username

        if o.password is not None:
            conn_kwargs["password"] = o.password

        if o.path.strip("/"):
            conn_kwargs["topics"] = o.path.strip("/").split(":")

        return cls(**conn_kwargs, **kwargs)

    def _create_client(self) -> aiomqtt.Client:
        """Create a MQTT client to communicate with server."""
        return aiomqtt.Client(
            hostname=self.hostname,
            port=self.port,
            username=self.username,
            password=self.password,
            transport=self.transport,
            logger=LOG,
            tls_context=create_default_context(),
            protocol=aiomqtt.ProtocolVersion.V5,  # WMO preference
        )

    async def iter_msgs(self):
        async with self._mqtt_client:
            LOG.info("Connected to '%s'.", self.hostname)
            for topic in self.topics:
                await self._mqtt_client.subscribe(topic, qos=1)
            async for msg in self._mqtt_client.messages:
                if msg.payload is None or isinstance(msg.payload, (float, int)):
                    LOG.info("Got non-JSON message from %s: %s", self.hostname, msg.payload)
                    continue
                try:
                    data = json.loads(msg.payload)
                except ValueError:
                    LOG.info("Got non-JSON message from %s: %s", self.hostname, msg.payload)
                    continue
                if not isinstance(data, dict):
                    LOG.info("Got non-JSON dictionary message from %s: %s", self.hostname, data)
                    continue
                if self.include_topic:
                    data["__topic__"] = str(msg.topic)
                yield data

    async def consume(self, into: asyncio.Queue) -> None:
        """Listens for all messages on the given connection and puts them into the queue."""
        remaining_attempts = self.reconnect_max
        while remaining_attempts:
            try:
                async for msg in self.iter_msgs():
                    await into.put(msg)
            except aiomqtt.MqttError:
                LOG.warning("Lost connection to %s. Reconnecting", self.hostname)
                await asyncio.sleep(self.reconnect_delay)


async def emit_json(msg, ident=None, end="\n"):
    """Default action. Print json string of message."""
    sys.stdout.write(json.dumps(msg, indent=ident) + end)
    sys.stdout.flush()


# TODO: have the option to download data using URL in payload.
# async def download(msg):
#     pass


def parse_cli_args():
    parser = argparse.ArgumentParser(prog="wiswatch", allow_abbrev=False)

    parser.add_argument("-V", "--version", action="store_true", help="Show version information and exit.")
    parser.add_argument("-v", "--verbose", action="count", default=None, help="Increase the verbosity of log output.")
    parser.add_argument("-q", "--quiet", action="store_true", help="Disable all log output to stderr.")
    parser.add_argument(
        "-0", "--null", action="store_true", help="Use NULL ('\\0') characters to separate output messages."
    )
    parser.add_argument(
        "-T",
        "--topic",
        action="store_true",
        help="Include the topic of the message in the payload as the key `__topic__`.",
    )
    parser.add_argument(
        "uris",
        nargs="*",
        help="Connection and/or subscription information: [{'mqtts'|'wss'}://][user]:[password]@[host]:[port][/topic]",
    )

    action_group = parser.add_argument_group("Actions")
    action_parser = action_group.add_mutually_exclusive_group(required=False)
    action_parser.add_argument(
        "-p",
        "--print",
        dest="action",
        action="store_const",
        const=emit_json,
        help="The default action. Print all message payloads.",
    )
    action_parser.add_argument(
        "-P",
        "--pprint",
        dest="action",
        action="store_const",
        const=functools.partial(emit_json, ident=2),
        help="Pretty print all message payloads.",
    )

    args = parser.parse_args()

    if args.version:
        sys.stdout.write(f"{parser.prog}: {__version__}\n")
        sys.stdout.flush()
        sys.exit()

    if args.verbose is not None and args.quiet:
        parser.error("Cannot specify both --verbose and --quiet!")

    if args.action is None:
        args.action = emit_json

    return args


async def consume_messages(action, queue: asyncio.Queue) -> None:
    while True:
        msg = await queue.get()
        await action(msg)


async def loop():
    args = parse_cli_args()

    if not args.quiet:
        log_levels = [logging.ERROR, logging.CRITICAL, logging.WARNING, logging.INFO, logging.DEBUG]
        logging.basicConfig(level=log_levels[min(args.verbose or 1, 4)])

    msg_queue = asyncio.Queue()

    if args.uris:
        cons = [WISConsumer.from_uri(uri, include_topic=args.topic) for uri in args.uris]
    else:
        cons = [WISConsumer(include_topic=args.topic)]

    async with asyncio.TaskGroup() as tg:
        tg.create_task(consume_messages(args.action, msg_queue))
        for con in cons:
            tg.create_task(con.consume(msg_queue))


def start():
    try:
        asyncio.run(loop())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    start()
