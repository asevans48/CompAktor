import atexit
import base64
import json
import pickle
import socket

import pytest

from actors.address.addressing import ActorAddress
from networking.socket_server import create_socket_server, ServerStopped, SocketServerSecurity
from networking.utils import package_message
from test.modules.message import TestMessage

HOST = '127.0.0.1'
PORT = 12000


@pytest.fixture
def socket_server():
    server = create_socket_server(HOST, PORT, 2)
    server.signal_queue.get(timeout=30)
    return server


@pytest.mark.order1
def test_socket_conn(socket_server):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(2)
        sock.connect((HOST, PORT))
    finally:
        sock.close()


@pytest.mark.order2
def test_send_message(socket_server):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sec = SocketServerSecurity()
        message = package_message(TestMessage(), ActorAddress('testa'), sec)
        sock.settimeout(2)
        sock.connect((HOST, PORT))
        sock.send(message)
        message = socket_server.message_queue.get(timeout=30)
        assert(type(message['sender']) is list)
        assert(message['sender'][0] == 'testa')
        assert(message.get('message', None) is not None)
        clz = base64.b64decode(message['message'])
        clz = pickle.loads(clz)
        assert(type(clz) == TestMessage)
        assert(clz.test_str == 'Hello')
        assert(clz.test_int == 1)
        assert(type(clz.test_dict) is dict)
    finally:
        sock.close()


@pytest.mark.order3
def test_stop_server(socket_server):
    socket_server.stop_server()
    msg = socket_server.signal_queue.get(timeout=30)
    assert(type(msg) == ServerStopped)
