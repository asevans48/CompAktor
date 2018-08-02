
import pytest

from actors.address.addressing import ActorAddress
from networking.communication import send_message
from networking.socket_server import create_socket_server, SocketServerSecurity
from networking.utils import package_message
from test.modules.message import TestMessage

HOST = '127.0.0.1'
PORT = 12000


@pytest.fixture
def socket_server():
    server = create_socket_server(HOST, PORT, 2)
    server.signal_queue.get(timeout=30)
    yield server
    server.stop_server()
    server.signal_queue.get(timeout=30)



def test_communication_with_server(socket_server):
    sender = ActorAddress('testa', HOST, PORT)
    message = TestMessage()
    sec = SocketServerSecurity()
    message = package_message(message, sender, sec, sender)
    send_message(message, sender, sender, sec)
    msg = socket_server.message_queue.get(timeout=30)
    assert(type(msg) is dict)
    assert(msg.get('message', False))
