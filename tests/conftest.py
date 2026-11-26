"""Unit tests must never authenticate or contact INPI/GCP."""

import socket

import pytest

# Local Spark: the Python driver talks to its own JVM over the loopback interface.
LOOPBACK = {"127.0.0.1", "::1", "localhost"}


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    connect, create_connection = socket.socket.connect, socket.create_connection

    def guarded_connect(self, address):
        if isinstance(address, tuple) and address[0] in LOOPBACK:
            return connect(self, address)
        raise AssertionError("Network access is forbidden in unit tests")

    def guarded_create_connection(address, *args, **kwargs):
        if address[0] in LOOPBACK:
            return create_connection(address, *args, **kwargs)
        raise AssertionError("Network access is forbidden in unit tests")

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket, "create_connection", guarded_create_connection)
