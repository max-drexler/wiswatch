# wiswatch

Consume data notifications from WMO's next-gen global information system (WIS 2.0).

## Description

`wiswatch` is a simple tool for receiving data status notifications from [the WMO Information System 2.0 (WIS 2.0)](#What-is-WIS2). It is built with simplicity and robustness in mind to ensure that messages are received reliably.

`wiswatch` can be used as a source in your data pipeline, or, depending on the use-case, the entire pipeline itself. By default it will print each message's json-encoded payload to stdout, but can be directed to download the data source the notification describes (and [more](#actions)).

## Install

### Requirements

- Python >= 3.12
- [aiomqtt](https://pypi.org/project/aiomqtt/) package (If directly copying `wiswatch.py` file.)

## Usage

After installation, the `wiswatch` command will be available. For a complete overview of all the options and abilities of `wiswatch`, use `wiswatch --help`.

### Connections & Topics

Without specifying any connection information or topics, `wiswatch` will connect to the global broker in France (`globalbroker.meteo.fr`) and consume all "core" data (topic: `cache/a/wis2/+/data/core/#`). This is not likely to be the desired behavior, so `wiswatch` accepts one or more URIs as positional arguments.

URIs must follow the general [RFC 2396](https://datatracker.ietf.org/doc/html/rfc2396.html) format, with the caveat that the scheme must be `mqtts` or `wss` (default is `mqtts`). Additionally, no single part of a URI is required. All missing values are replaced with defaults.

A URI's path is interpreted as the topic to subscribe to for that connection. Multiple topics can be subscribed to on the same connection by separting the path components with a `:`. For example, `wiswatch /data/one/:/data/two/` will use the default connection information and subscribe to the topics `/data/one/` and `/data/two/`.

The (non-normative) URI format is as follows:

```bash
[{"mqtts" | "wss"}"://"] [[user] [":" password] "@" ] [host] [":" port] ["/" topic] ["/:/" another/topic]
```

When multiple URIs are specified, `wiswatch` will consume messages from all connections in parallel.

If you are unsure if your URIs are being parsed properly, use the `--explain` option to show how `wiswatch` interpreted them.

### Actions



### Wiswatch-Specifc Payload Keys

By default `wiswatch` adds additional metadata to every payload it receives. This information is included in the `properties` portion of the payload so WNM conformance is not broken.

The data added is as follows:

| Key | Value |
| :-: | :-: |
| `properties.__topic__` | The MQTT topic the message was published with. |
| `properties.__reception_time__` | ISO-formatted timestamp when the messages was received by `wiswatch`. |
| `properties.__reception_host__` | The hostname for the global broker/cache the messages was received from. |

To disable this behavior, use the `-R`/`--raw-payload` cli option.

### Python Library

The main Python interface is the `WISConsumer` class.

```python3
import asyncio

from wiswatch import WISConsumer

async def main():
  client = WISConsumer(topics=['/cache/a/wis2/+/data/weather/'])
  async for msg in client.iter_msgs():
    print(msg)

if __name__ == '__main__':
  asyncio.run(main())
```

## What is WIS2

From the [WMO](https://community.wmo.int/en/activity-areas/wis/wis2-implementation):

> WIS 2.0 provides a framework for WMO data sharing in the 21st century, for all WMO members and all the WMO disciplines in domains to embrace the Earth system approach, enable the WMO unified data policy, and support the WMO global basic observing network.

Basically, a global system for sharing meteorological/geospatial data.

## Author

Created by [Max Drexler](mailto:mndrexler@wisc.edu) with heavy inspiration from [amqpfind]() by [Ray Garcia]().

## License

Created under the MIT license, see [LICENSE](/LICENSE) for more information.
