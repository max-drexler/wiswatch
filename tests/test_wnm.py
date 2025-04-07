"""tests.test_wnm

Unit tests for WISMessage class.
"""

import json
from contextlib import nullcontext

import pytest

from wiswatch import WISMessage, WNMConformanceError

TEST_MESSAGE = {
    "id": "62b1d4b2-39f3-4588-a1bb-6aca4df0cd4c",
    "type": "Feature",
    "conformsTo": ["http://wis.wmo.int/spec/wnm/1/conf/core"],
    "geometry": None,
    "properties": {
        "data_id": "wis2/ca-eccc-msc/data/core/weather/experimental/SRWA20_KWAL_041646___1308",
        "pubtime": "2025-04-04T16:46:34Z",
        "integrity": {
            "method": "sha512",
            "value": "lSp94ExGM2X0bqoKm7ev7xQsyDgHiB2jgJJYwv8sVRYAaQ0FTdjUuv/QUPRLcXuL3S/CQM7/fH9q5I4akTPrZw==",
        },
        "datetime": None,
        "metadata_id": "urn:wmo:md:ca-eccc-msc:02fd8740-af64-4ce0-af42-3c17c92cf02b",
        "cache-id": "cn-cma-global-cache",
        "__topic__": "cache/a/wis2/ca-eccc-msc/data/core/weather/experimental",
        "__reception_time__": "2025-04-04T16:46:35.145653+00:00",
        "__reception_host__": "globalbroker.meteo.fr",
    },
    "links": [
        {
            "rel": "canonical",
            "type": "text/plain",
            "href": "https://gc.wis.cma.cn/20250404/cache/a/wis2/ca-eccc-msc/data/core/weather/experimental/SRWA20_KWAL_041646___1308",
            "length": 163,
        }
    ],
}


@pytest.mark.parametrize(
    (
        "key",
        "expectation",
    ),
    [
        ("id", nullcontext(TEST_MESSAGE["id"])),
        ("properties.pubtime", nullcontext(TEST_MESSAGE["properties"]["pubtime"])),
        ("properties.integrity.method", nullcontext(TEST_MESSAGE["properties"]["integrity"]["method"])),
        ("@access_link", nullcontext(TEST_MESSAGE["links"][0])),
        ("@access_link.length", nullcontext(TEST_MESSAGE["links"][0]["length"])),
        ("@canonical_url", nullcontext(TEST_MESSAGE["links"][0]["href"])),
        ("@nothing", pytest.raises(AttributeError)),
        ("@", pytest.raises(AttributeError)),
        ("@...", pytest.raises(AttributeError)),
        ("properties.unknown", pytest.raises(KeyError)),
    ],
)
def test_get_item(key, expectation):
    """Use nullcontext to pass a value to check or pytest.raises for catching exceptions."""
    with expectation as e:
        assert WISMessage(**TEST_MESSAGE)[key] == e


@pytest.mark.parametrize("data", [{}, [], {"missing-keys": "k"}])
def test_conformance_error(data):
    """Invalid data should lazily raise WNMConformance errors."""
    with pytest.raises(WNMConformanceError):
        WISMessage.from_json(json.dumps(data))
