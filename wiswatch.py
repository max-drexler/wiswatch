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
from dataclasses import dataclass, field
from ssl import create_default_context
from urllib.parse import urlparse

import aiomqtt

LOG = logging.getLogger("wiswatch")


def port_per_transport(transport: str) -> int:
    if transport.lower() in ("tcp", "mqtt", "mqtts"):
        return 8883
    if transport.lower() in ("websockets", "websocket", "ws", "wss"):
        return 443
    msg = f"Unknown transport: {transport}"
    raise ValueError(msg)


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

        conn_kwargs: dict = {"transport": transport}
        if host is not None:
            conn_kwargs["hostname"] = host

        if port is not None:
            conn_kwargs["port"] = port

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


def format_emit(fmt_str: str):
    async def emitter(msg):
        sys.stdout.write(fmt_str.format_map(msg) + '\n')
        sys.stdout.flush()
    return emitter


# TODO: have the option to download data using URL in payload.
# async def download(msg):
#     pass

WISWATCH_EPILOG = """For more information on WIS2, see the following:
    - Topic hierarchy: https://github.com/wmo-im/wis2-topic-hierarchy/tree/main
    - Payload format: https://wmo-im.github.io/wis2-notification-message/standard/wis2-notification-message-STABLE.html
"""


def parse_cli_args():
    parser = argparse.ArgumentParser(
        prog="wiswatch",
        allow_abbrev=False,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=WISWATCH_EPILOG,
    )

    parser.add_argument("-V", "--version", action="store_true", help="Show version information and exit.")
    # parser.add_argument(
    #     "-0", "--null", action="store_true", help="Use NULL ('\\0') characters to separate output messages."
    # )
    parser.add_argument(
        "-T",
        "--topic",
        action="store_true",
        help="Include the topic of the message in the payload as the key `__topic__`.",
    )
    parser.add_argument("--explain", action="store_true", help="Show parsed action/connection info and exit.")
    parser.add_argument(
        "uris",
        nargs="*",
        help="Connection and/or subscription information: [{'mqtts'|'wss'}://][user]:[password]@[host]:[port][/topic]",
    )

    log_parser = parser.add_argument_group("Log Options")
    verbosity_group = log_parser.add_mutually_exclusive_group(required=False)
    verbosity_group.add_argument(
        "-v",
        "--verbose",
        dest="verbosity",
        metavar="LEVEL",
        choices=["DEBUG", "INFO", "WARNING", "CRITICAL", "ERROR"],
        default="WARNING",
        help="Verbosity of %(prog)s (ERROR, CRITICAL, WARNING, INFO, DEBUG). Default %(default)s.",
    )
    # If -q is specified args.verbosity will be None
    verbosity_group.add_argument(
        "-q", "--quiet", dest="verbosity", action="store_const", const=None, help="Disable all log output to stderr."
    )

    # Only allowed to choose one action
    # args.action must be a coroutine that accepts one argument, the message.
    action_group = parser.add_argument_group(
        "Actions", "Choose one action to perform on all messages. Default is --print"
    )
    action_parser = action_group.add_mutually_exclusive_group(required=False)
    # Follow these kwargs for adding an action that doesn't accept args
    action_parser.add_argument(
        "-p",
        "--print",
        dest="action",
        action="store_const",
        const=emit_json,
        help="Print JSON-serialized message payloads.",
    )
    action_parser.add_argument(
        "-P",
        "--pprint",
        dest="action",
        action="store_const",
        const=functools.partial(emit_json, ident=2),
        help="Pretty print message payloads.",
    )
    # Follow these kwargs for adding an action that accepts arguments
    action_parser.add_argument(
        "-F",
        "--fprint",
        metavar="FSTRING",
        dest="action",
        type=format_emit,
        help="Print a string based on keys in the payload. E.g. '{properties.data_id}:{properties.start_time}'",
    )

    args = parser.parse_args()

    if args.version:
        sys.stdout.write(f"{parser.prog}: {__version__}\n")
        sys.stdout.flush()
        sys.exit()

    if args.action is None:
        args.action = emit_json

    return args


async def consume_messages(action, queue: asyncio.Queue) -> None:
    while True:
        msg = await queue.get()
        await action(msg)


async def loop():
    args = parse_cli_args()

    if args.verbosity is not None:
        logging.basicConfig(level=args.verbosity)

    msg_queue = asyncio.Queue()

    if args.uris:
        cons = [WISConsumer.from_uri(uri, include_topic=args.topic) for uri in args.uris]
    else:
        cons = [WISConsumer(include_topic=args.topic)]

    if args.explain:
        sys.stdout.write(f'Action: {args.action.__name__}\n')
        sys.stdout.write("Connection(s):\n")
        sys.stdout.write("\t\n".join(map(str, cons)))
        sys.stdout.write("\n")
        sys.exit(0)

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
