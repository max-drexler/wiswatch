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
import typing as tp
from ssl import create_default_context
from urllib.parse import urlparse

LOG = logging.getLogger(__name__)

if tp.TYPE_CHECKING:
    MQTTScheme = tp.Literal["wss", "mqtts"]

    class MQTTConnectionInfo(tp.TypedDict):
        hostname: str
        port: int
        username: str
        password: str
        transport: tp.Literal["tcp", "websockets"]
        topic: str


# Connection URI information
TRANSPORT_PER_SCHEME: dict[str, tp.Literal["websockets", "tcp"]] = {
    "mqtts": "tcp",
    "wss": "websockets",
}

PORT_PER_SCHEME = {"mqtts": 8883, "wss": 443}

DEFAULT_PASSWORD = "everyone"  # noqa: S105
DEFAULT_USERNAME = "everyone"
DEFAULT_HOSTNAME = "globalbroker.meteo.fr"


def default_mqtt_connection(scheme: MQTTScheme | None = None) -> MQTTConnectionInfo:
    return {
        "hostname": DEFAULT_HOSTNAME,
        "password": DEFAULT_PASSWORD,
        "port": PORT_PER_SCHEME[scheme or "mqtts"],
        "topic": "cache/a/wis2/+/data/core/#",
        "transport": TRANSPORT_PER_SCHEME[scheme or "mqtts"],
        "username": DEFAULT_USERNAME,
    }


async def emit_json(msg, ident=None, end="\n"):
    """Default action. Print json string of message."""
    sys.stdout.write(json.dumps(msg, indent=ident) + end)
    sys.stdout.flush()


# TODO: have the option to download data using URL in payload.
# async def download(msg):
#     pass


def parse_mqtt_uri(uri: str, default_scheme: tp.Literal["mqtts", "wss"] | None = None) -> MQTTConnectionInfo:
    """Parses the URI and return the kwargs to make the connection using ``aiomqtt.Client``."""
    try:
        o = urlparse(uri, allow_fragments=False)
    except (TypeError, ValueError) as e:
        err_msg = f"Invalid URI: {uri}"
        LOG.critical(err_msg)
        raise ValueError(err_msg) from e

    if not o.scheme:
        transport = TRANSPORT_PER_SCHEME[default_scheme or "mqtts"]
    elif o.scheme not in PORT_PER_SCHEME:
        err_msg = f"Invalid scheme '{o.scheme}', must be one of {', '.join(PORT_PER_SCHEME.keys())}."
        LOG.critical(err_msg)
        raise ValueError(err_msg)
    else:
        transport = TRANSPORT_PER_SCHEME[o.scheme]

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

    return {
        "transport": transport,
        "password": o.password if o.password is not None else DEFAULT_PASSWORD,
        "username": o.username if o.username is not None else DEFAULT_USERNAME,
        "port": port if port is not None else PORT_PER_SCHEME[o.scheme or "mqtts"],
        "hostname": host if host is not None else DEFAULT_HOSTNAME,
        "topic": o.path.removeprefix("/"),
    }


def parse_cli_args():
    parser = argparse.ArgumentParser(prog="wiswatch", allow_abbrev=False)

    parser.add_argument("--version", action="store_true", help="Show version information and exit.")
    parser.add_argument("-v", "--verbose", action="count", default=None, help="Increase the verbosity of log output.")
    parser.add_argument("-q", "--quiet", action="store_true", help="Disable all log output to stderr.")
    parser.add_argument(
        "--ws",
        action="store_true",
        help=(
            "URIs w/o a scheme default to MQTT over WebSocket. Alternatively, can be"
            "specified on a per-uri basis using the wss:// scheme."
        ),
    )
    parser.add_argument(
        "-0", "--null", action="store_true", help="Use NULL ('\\0') characters to separate output messages."
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

    default_scheme = "wss" if args.ws else "mqtts"
    args.conn_list = [parse_mqtt_uri(uri, default_scheme=default_scheme) for uri in args.uris]
    if not args.conn_list:
        args.conn_list = [default_mqtt_connection(default_scheme)]

    return args


async def produce_mqtt(con_info: MQTTConnectionInfo, queue: asyncio.Queue) -> tp.AsyncIterator[dict]:
    """Consume all messages from a MQTT connection and put them in a queue."""
    import aiomqtt

    client = aiomqtt.Client(
        hostname=con_info["hostname"],
        port=con_info["port"],
        username=con_info["username"],
        password=con_info["password"],
        transport=con_info["transport"],
        logger=LOG,
        tls_context=create_default_context(),
        protocol=aiomqtt.ProtocolVersion.V5,  # WMO preference
    )

    while True:
        try:
            async with client:
                LOG.info("Connected to '%s'.", con_info["hostname"])
                await client.subscribe(con_info["topic"], qos=1)
                async for msg in client.messages:
                    if msg.payload is None or isinstance(msg.payload, (float, int)):
                        LOG.info("Got non-JSON message from %s: %s", con_info["hostname"], msg.payload)
                        continue
                    try:
                        data = json.loads(msg.payload)
                    except ValueError:
                        LOG.info("Got non-JSON message from %s: %s", con_info["hostname"], msg.payload)
                        continue
                    if not isinstance(data, dict):
                        LOG.info("Got non-JSON dictionary message from %s: %s", con_info["hostname"], data)
                        continue
                    await queue.put(data)
        except aiomqtt.MqttError:
            LOG.warning("Lost connection to %s. Reconnecting", con_info["hostname"])
            await asyncio.sleep(3)


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

    async with asyncio.TaskGroup() as tg:
        tg.create_task(consume_messages(args.action, msg_queue))
        for con in args.conn_list:
            tg.create_task(produce_mqtt(con, msg_queue))


if __name__ == "__main__":
    try:
        asyncio.run(loop())
    except KeyboardInterrupt:
        sys.exit(0)
