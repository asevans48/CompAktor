
import pytest

from networking.socket_server import start_socket_server


@pytest.fixture
def socket_server():
    server = start_socket_server('127.0.0.1', 8080)
    server.start()
    yield server
    server.join(2)

