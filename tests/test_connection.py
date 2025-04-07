"""tests.test_connection

Unit tests for WISConnection class.
"""

from contextlib import nullcontext

import pytest

from wiswatch import WISConnection


@pytest.mark.parametrize(
    "uri, expectation",
    [
        ("/topic1/", nullcontext(WISConnection(topics=["topic1"]))),
        ("/topic1/:/topic2/", nullcontext(WISConnection(topics=["topic1", "topic2"]))),
        ("/topic1/#", nullcontext(WISConnection(topics=["topic1/#"]))),
        ("/topic1/+/+/#", nullcontext(WISConnection(topics=["topic1/+/+/#"]))),
        ("wss://", nullcontext(WISConnection(transport='websockets'))),
        ("ws://", nullcontext(WISConnection(transport='websockets'))),
        ("mqtts://user:pwd@", nullcontext(WISConnection(username="user", password="pwd"))),
        ("mqtts://user@test", nullcontext(WISConnection(username="user", hostname="test"))),
        ("mqtts://test:100", nullcontext(WISConnection(port=100, hostname="test"))),
        ("mqtts://host:port", pytest.raises(ValueError)),
        ("http://test", pytest.raises(ValueError)),
        ("mqtts://[:asdf", pytest.raises(ValueError)),
        ("mqtts://[:asdf", pytest.raises(ValueError)),
    ],
)
def test_from_uri(uri, expectation):
    """Ensure that connections constructed from URIs follow expectations.

    expectation is context manager: use nullcontext to pass value, pytest.raises to assert errors.
    """
    with expectation as e:
        assert WISConnection.from_uri(uri) == e
