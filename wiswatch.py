#! /usr/bin/env python3

"""wiswatch: A CLI to consume data notifications from WMO's Information System 2.0 (WIS2).

Copyright (C) 2025 Max Drexler

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import annotations

__version__ = "0.1.0"
__author__ = "Max Drexler"
__email__ = "mndrexler@wisc.edu"

import argparse
import asyncio
import base64
import contextlib
import functools
import gzip
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from ssl import create_default_context
from typing import TYPE_CHECKING, Any, AsyncContextManager, AsyncIterator, Callable
from urllib.parse import urlparse

import aiohttp
import aiomqtt

if TYPE_CHECKING:
    from types import CoroutineType
    from typing import Mapping, TypedDict

    # WISDispatcher types
    StatelessContext = Callable[[], AsyncContextManager[None]]
    StatefullContext = Callable[[], AsyncContextManager[Mapping[str, object]]]
    ContextFunction = StatelessContext | StatefullContext

    StatelessDispatch = Callable[["WISMessage"], CoroutineType]
    StatefullDispatch = Callable[[Mapping[str, Any], "WISMessage"], CoroutineType]
    DispatchFunction = StatelessDispatch | StatefullDispatch

    # Context when downloading WIS2 data
    class DownloadContext(TypedDict):
        session: aiohttp.ClientSession


LOG = logging.getLogger("wiswatch")


class WNMConformanceError(ValueError):
    """An MQTT payload wasn't conformant with the WNM specification

    https://wmo-im.github.io/wis2-notification-message/standard/wis2-notification-message-STABLE.html
    """


class WISMessage(Mapping):
    """A bare-bones wrapper around a dictionary that provides:
        - lazy WNM validation
        - useful formatting syntax
        - instance methods
    specifically for WIS2 Notification Messages.
    """

    __slots__ = ("__access_link", "__canon_url", "_data")

    def __init__(self, *args, **kwargs) -> None:
        self._data: dict[str, Any] = {}
        self._data.update(*args, **kwargs)
        self.__access_link = None
        self.__canon_url = None

    @classmethod
    def from_json(cls, s: str | bytes | bytearray) -> WISMessage:
        """Load a WISMessage from a JSON string, bytes, or bytearray.

        Raises:
            JSONDecodeError: Non-JSON input.
            WNMConformanceError: JSON object missing required field(s).
        """

        def _decode_wnm(d: dict):
            """Just turn the top-level dictionary into a WISMessage."""
            if all(req_key in d for req_key in ("id", "links", "properties", "geometry", "type")):
                return cls(**d)
            return d

        obj = json.loads(s, object_hook=_decode_wnm)

        # this happens for valid json where no object is turned into a WISMessage in ``_decode_wnm``
        if not isinstance(obj, cls):
            # TODO: better error message
            err = "Missing a required key"
            raise WNMConformanceError(err)

        return obj

    def to_json(self, **kwargs) -> str:
        """Turn WISMessage into a JSON string.

        Args:
            **kwargs: same as json.dumps.
        """
        return json.dumps(self._data, **kwargs)

    @property
    def access_link(self):
        """The link object with the 'canonical' reference to the data.

        Raises:
            WNMConformanceError: The data the this WISMessage describes isn't conformant.
        """
        # TODO: track conformance, so re-checking isn't necessary
        if self.__access_link is None:
            links = self._data.get("links")
            if links is None:
                raise WNMConformanceError("Missing required 'links' property")
            if not isinstance(links, list):
                raise WNMConformanceError(f"'links' property is of type '{type(links).__name__}', exepected 'list'")

            for link in links:
                if not isinstance(link, dict):
                    raise WNMConformanceError(f"link {link} in 'links' is {type(link).__name__} not dict")

                link_rel = link.get("rel")
                if link_rel == "canonical":
                    self.__access_link = link
                    return self.__access_link
            raise WNMConformanceError("No 'canonical' link in 'links'")

        return self.__access_link

    @property
    def canonical_url(self) -> str:
        """URL to data the notification is describing.

        Raises:
            WNMConformanceError: The data the this WISMessage describes isn't conformant.
        """
        if self.__canon_url is None:
            url = self.access_link.get("href")
            if url is None or not isinstance(url, str):
                raise WNMConformanceError("Missing 'href' property for canonical link")
            if not url.startswith(("http://", "https://", "ftp://", "sftp://")):
                raise WNMConformanceError("'href' for canonical link not http/s or s/ftp")
            self.__canon_url = url

        return self.__canon_url

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, key: str) -> Any:
        """Supports 'multi-keys', e.g. WISMessage['key.subkey'], and 'quick keys',
        e.g. WISMessage['@access_link'].
        """
        if not isinstance(key, str):
            err = f"key must be type 'str', got {type(key).__name__}"
            raise TypeError(err)

        if key.startswith("@"):
            # A 'quick' key, uses a property to get value
            # This is mostly useful when creating a format string
            #   e.g. '{@access_link}'.format_map(WISMessage)

            qkey, *multi = key[1:].split(".")

            # AttributeError should be raises, user error with format string
            data = getattr(self, qkey)
        else:
            multi = key.split(".")
            data = self._data

        return reduce(dict.__getitem__, multi, data)

    async def iter_data(
        self, session: aiohttp.ClientSession | None = None, chunk_size: int = 2048
    ) -> AsyncIterator[bytes]:
        """Iterate the actual remote data described by this WISMessage.

        This is either done by streaming byte content from the HTTP/FTP canonical url,
        or yielding the data directly if it is inlined in the notification.

        Args:
            session (aiohttp.ClientSession | None): An asynchronous HTTP client, one will be created if not specified.
            Recommended to construct your own client for better performance. Default None.
            chunk_size (int): Size of byte content to yield. Default 2048.

        Yields:
            bytes: Chunks of the data content described by the WIS2 Notification Message.
        """
        inline = self.get("properties.content")

        if inline:
            if not isinstance(inline, dict):
                err = f"'properties.content' is {type(inline).__name__}, expected a dict"
                LOG.warning(err)
                raise WNMConformanceError(err)

            encoding = str(inline.get("encoding", "utf-8")).lower()
            content = inline.get("value")
            if content is None:
                # Undefined in WNM standard, could raise an error, or fallback on links
                raise WNMConformanceError("'properties.content.value' doesn't exist")
            elif encoding == "utf-8":
                b = content
            elif encoding == "gzip":
                b = gzip.decompress(content)
            elif encoding == "base64":
                b = base64.b64decode(content)
            else:
                raise WNMConformanceError(
                    "Unknown 'properties.content.encoding' '{encoding}', expected 'utf-8', 'gzip', or' base64'"
                )

            for i in range(0, len(b), chunk_size):
                yield b[i:i]
            return

        session = aiohttp.ClientSession() if session is None else session
        async with session.get(self.canonical_url) as resp:
            async for chunk in resp.content.iter_chunked(chunk_size):
                yield chunk


def port_per_transport(transport: str) -> int:
    if transport.lower() in ("tcp", "mqtt", "mqtts"):
        return 8883
    if transport.lower() in ("websockets", "websocket", "ws", "wss"):
        return 443
    msg = f"Unknown transport: {transport}"
    raise ValueError(msg)


def all_open_data():
    """Topics for receiving all 'core' (free/open) data."""
    return ["cache/a/wis2/+/data/core/#"]


@dataclass
class WISConnection:
    # Connection kwargs
    hostname: str = field(default="globalbroker.meteo.fr")
    topics: list[str] = field(default_factory=all_open_data)
    port: int | None = field(default=None)
    username: str = field(default="everyone")
    password: str = field(default="everyone", repr=False)
    transport: str = field(default="tcp")

    # Non-connection kwargs
    msg_callback: Callable[[WISMessage, aiomqtt.Message, WISConnection], CoroutineType] | None = field(
        default=None, repr=False
    )
    reconnect_delay: float = field(default=3.0)
    reconnect_max: int = field(default=-1)

    _mqtt_client: aiomqtt.Client = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.port = port_per_transport(self.transport)
        self._mqtt_client = self._create_client()

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> WISConnection:
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
            conn_kwargs["topics"] = o.path.strip("/").split("/:/")

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

    async def iter_msgs(self) -> AsyncIterator[WISMessage]:
        """Asynchronously iterate over all messages received on this connection."""
        async with self._mqtt_client:
            LOG.debug("%s connected", self)
            for topic in self.topics:
                await self._mqtt_client.subscribe(topic, qos=1)

            async for msg in self._mqtt_client.messages:
                if msg.payload is None or isinstance(msg.payload, (float, int)):
                    LOG.info("%s got non-JSON MQTT payload", self)
                    continue
                try:
                    wnm = WISMessage.from_json(msg.payload)
                except WNMConformanceError as e:
                    LOG.info("%s got non-conformant MQTT payload: %s", self, str(e))
                    continue

                if self.msg_callback is not None:
                    await self.msg_callback(wnm, msg, self)
                yield wnm

    async def consume_into(self, into: asyncio.Queue[WISMessage]) -> None:
        """Listens for all messages on the given connection and puts them into the queue."""
        remaining_attempts = self.reconnect_max
        while remaining_attempts:
            try:
                async for msg in self.iter_msgs():
                    LOG.debug("%s got message: %s", self, msg)
                    await into.put(msg)
            except aiomqtt.MqttError:
                LOG.warning("%s lost connection. Reconnecting in %d.", self, self.reconnect_delay)
                await asyncio.sleep(self.reconnect_delay)

    def __str__(self) -> str:
        return f"WISConnection({self.username}@{self.hostname}, topics='{'\', \''.join(self.topics)}')"


@contextlib.asynccontextmanager
async def http_session() -> AsyncIterator[DownloadContext]:
    """A context manager for WISDispatcher that yields a http (eventually ftp also) client."""
    async with aiohttp.ClientSession() as session:
        yield {"session": session}


async def wis2_data_download(session: aiohttp.ClientSession, directory: str, msg: dict) -> None:
    """Download the data a WIS2 notification is describing."""
    props = msg.get("properties")
    if props is None:
        LOG.warning("Invalid WIS2 message: missing 'properties' key! %s", msg)
        return

    inline = props.get("content")
    if inline is not None:
        LOG.critical("Inline download not yet supported!")
        return

    links = msg.get("links")
    if links is None:
        LOG.warning("Invalid WIS2 message: missing 'links' key! %s", msg)
        return
    if not isinstance(links, list):
        LOG.warning("Invalid WIS2 message: 'links' value is not list, got %s", type(links).__name__)
        return

    # Find canonical link
    rel_link = None
    for link in links:
        if not isinstance(link, dict):
            LOG.warning("Invalid WIS2 message: link %s is %s not dict", link, type(link).__name__)
            # Could continue iterating, but why support non-conformant messages?
            return

        relation = link.get("rel")
        if relation in ("update", "deletion"):
            LOG.info("wiswatch doesn't currently support update/deletion notifications")
            # TODO: support updating files, optionally support deleting old files.
            return
        if relation == "canonical":
            if rel_link is not None:
                LOG.warning("WIS2 notification has multiple canonical links, defaulting to the last one, %s", links)
            rel_link = link

    if rel_link is None:
        # Couldn't find canonical link
        LOG.warning("Invalid WIS2 message: no canonical link in 'links' %s", links)
        return

    url = rel_link.get("href")
    if url is None or not isinstance(url, str) or not url.startswith(("http://", "https://", "ftp://", "sftp://")):
        LOG.warning("Invalid WIS2 message: non-valid canonical url '%s'", url)
        return

    try:
        o = urlparse(url)
    except (TypeError, ValueError) as e:
        LOG.warning("Invalid WIS2 message: canonical url couldn't be parsed '%s'", str(e))
        return

    # Stream file remote content to file
    download_file = os.path.join(directory, os.path.basename(o.path))
    if os.path.isfile(download_file):
        LOG.critical("WIS2 data file '%s' already exists!", download_file)
        return

    # TODO: suppress SIGINT to avoid file corruption
    with open(download_file, "wb") as f:  # noqa: ASYNC101 (might need to come back to this)
        async with session.get(url) as resp:
            async for chunk in resp.content.iter_chunked(2048):
                f.write(chunk)

    # Verify integrity of file
    #


@dataclass
class WISDispatcher:
    """Dispatch WIS2 messages to an async def function, optionally, with context.

    ``dispatch_function`` must be an asyc def function that accepts a WIS2 message (dictionary) and
    context in the form of a dictionary (if ``context_function`` is used and returns a non-None value).

    ``context_function`` is optional. If used, it should be an async context manager. Its context
    is established before any message is dispatched and torn down before program exit.
    ``context_function`` can yield a dictionary that will be passed to the ``dispatch_function``
    as an argument, or None if ``dispatch_function`` doesn't need context.

    Example:

    ```python
    from contextlib import asynccontextmanager
    from typing import AsyncContextManager, TypedDict


    # To ensure types are valid
    class MyContext(TypedDict):
        session: aiohttp.ClientSession


    @asynccontextmanager
    def my_context() -> AsyncIterator[MyContext]:
        with aiohttp.session() as sesh:
            yield {"session": sesh}


    async def my_dispatch(context: MyContext, message: dict) -> None:
        await context["session"].get(message["url"])


    dispatcher = WISDispatcher(my_dispatch, my_context)
    ```
    """

    dispatch_function: DispatchFunction
    context_function: ContextFunction = field(default=lambda: contextlib.nullcontext(None))

    def __post_init__(self) -> None:
        LOG.debug("Created %s", self)

    async def dispatch_from(self, _from: asyncio.Queue[WISMessage]) -> None:
        """Dispatch WIS2 notification received from the queue to the action function."""

        async with self.context_function() as context:
            # If no context is given, don't pass to dispatch function
            f = self.dispatch_function if context is None else functools.partial(self.dispatch_function, context)

            while True:
                msg = await _from.get()
                await f(msg)

    ## Built-in dispatch functions ##

    @classmethod
    def print_messages(cls, indent: int | None = None, end: str = "\n") -> WISDispatcher:
        """WISDispatcher that prints JSON-encoded payloads to stdout."""

        async def print_wis(msg: WISMessage):
            """Default action. Print json string of message."""
            sys.stdout.write(json.dumps(msg, indent=indent) + end)
            sys.stdout.flush()

        return cls(print_wis)

    @classmethod
    def fprint_messages(cls, format_str: str) -> WISDispatcher:
        """WISDispatcher that uses WIS2 notification payloads to populate a format string, and prints that."""

        async def fmt_print(msg: WISMessage):
            sys.stdout.write(format_str.format_map(msg) + "\n")
            sys.stdout.flush()

        return cls(fmt_print)

    @classmethod
    def download_data(cls, directory: str) -> WISDispatcher:
        if not os.path.isdir(os.path.abspath(directory)):
            msg = f"Directory {directory} doesn't exist!"
            LOG.critical(msg)
            raise OSError(msg)

        # This is a little too much shenanigans for my taste, will have to refactor eventually
        #   => wrapper inside a classmethod that calls other method...
        async def download_wrapper(context: DownloadContext, msg: WISMessage):
            await wis2_data_download(context["session"], directory, msg)

        return cls(
            download_wrapper,
            http_session,
        )


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
        "-r",
        "--raw-payload",
        action="store_true",
        help=(
            "Remove wiswatch-created payload keys: properties.__topic__, "
            "properties.__reception_time__, and properties.__reception_host__."
        ),
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
        "Actions", "An action to perform on each message. Default is to print to stdout."
    )
    action_parser = action_group.add_mutually_exclusive_group(required=False)

    # Follow these kwargs for adding an action that doesn't accept args
    action_parser.add_argument(
        "-P",
        "--pprint",
        dest="action",
        action="store_const",
        const=WISDispatcher.print_messages(indent=2),
        help="Pretty print message payloads.",
    )
    # Follow these kwargs for adding an action that accepts arguments
    action_parser.add_argument(
        "-F",
        "--fprint",
        metavar="FSTRING",
        dest="action",
        type=WISDispatcher.fprint_messages,
        help="Print a formatted string based on keys in the payload. E.g. '{properties.data_id}:{properties.start_time}'",
    )
    action_parser.add_argument(
        "-D",
        "--download",
        metavar="DIR",
        dest="action",
        type=WISDispatcher.download_data,
        help="Download the file data described in each WIS2 payload to a directory. File name is automatically determined from the payload.",
    )

    args = parser.parse_args()

    if args.version:
        parser.exit(0, f"{parser.prog}: {__version__}")

    if args.action is None:
        args.action = WISDispatcher.print_messages()

    return args


async def add_msg_defaults(data: dict, msg: aiomqtt.Message, client: WISConnection) -> None:
    """Add additional information to the payload."""

    data.get("properties", {}).update(
        __topic__=str(msg.topic),
        __reception_time__=datetime.now(tz=timezone.utc).isoformat(),
        __reception_host__=client.hostname,
    )


async def loop():
    args = parse_cli_args()

    if args.verbosity is not None:
        logging.basicConfig(level=args.verbosity)

    msg_queue = asyncio.Queue()

    msg_clbk = add_msg_defaults if not args.raw_payload else None

    if args.uris:
        cons = [WISConnection.from_uri(uri, msg_callback=msg_clbk) for uri in args.uris]
    else:
        cons = [WISConnection(msg_callback=msg_clbk)]

    if args.explain:
        sys.stdout.write(f"Action: {args.action}\n")
        sys.stdout.write("Connection(s):\n")
        sys.stdout.write("\t\n".join(map(str, cons)))
        sys.stdout.write("\n")
        sys.exit(0)

    # TODO: make this backward-compatible with older python versions
    async with asyncio.TaskGroup() as tg:
        tg.create_task(args.action.dispatch_from(msg_queue))
        for con in cons:
            tg.create_task(con.consume_into(msg_queue))


def start():
    try:
        # If uvloop is available, use that
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass

    # TODO: signal handling
    try:
        asyncio.run(loop())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    sys.exit(start())
