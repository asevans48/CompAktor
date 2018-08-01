import json

import pytest

from actors.address.addressing import ActorAddress
from networking.socket_server import create_socket_server
from networking.utils import send_message_to_actor, send_json_to_actor


@pytest.fixture
def server():
    server = create_socket_server('127.0.0.1', 12000)
    server.start()
    server.signal_queue.get(timeout=30)
    yield server
    server.stop_server()
    server.signal_queue.get(timeout=30)


@pytest.fixture
def target():
    addr = ActorAddress('actora')
    addr.port = 12000
    addr.host = '127.0.0.1'
    return addr


@pytest.fixture
def sender():
    addr = ActorAddress('actorb')
    addr.port = 12000
    addr.host = '127.0.0.1'
    return addr


def test_send_string(server, target, sender):
    msg = "Hello World"
    send_message_to_actor(msg, target, sender)
    msg = server.message_queue.get(timeout=15)
    assert(msg is not None)
    assert(msg.get('data', None) is not None)
    msg = msg['data'].decode()
    assert(type(msg) is str)
    msg = json.loads(msg)
    assert('actora' in msg['target'])
    assert('actorb' in msg['sender'])
    assert("Hello World" in msg['message'])


def test_send_json(server, target, sender):
    msg = {'msg': 'Hello World'}
    send_json_to_actor(msg, target, sender)
    msg = server.message_queue.get(timeout=15)
    assert(msg is not None)
    assert(msg.get('data', None) is not None)
    msg = msg['data'].decode()
    assert (type(msg) is str)
    msg = json.loads(msg)
    assert ('actora' in msg['target'])
    assert ('actorb' in msg['sender'])
    assert(msg.get('msg', None) is not None)
    assert(msg['msg'] == 'Hello World')
