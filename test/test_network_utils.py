import base64
import hmac
import json
import pickle

import pytest

from actors.address.addressing import ActorAddress
from networking.socket_server import create_socket_server, SocketServerSecurity
from networking.utils import package_message, send_message_to_actor
from test.modules.message import TestMessage


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

@pytest.fixture
def security_config():
    return SocketServerSecurity()


def test_package_message(security_config):
    message = TestMessage()
    addr = ActorAddress('teseta')
    msg = package_message(message, addr, security_config)
    msgarr = msg.decode().split(':::', maxsplit=4)
    jarr = json.loads(msgarr[3])
    msg_instance = pickle.loads(base64.b64decode(jarr['message']))
    assert(type(msg_instance) == TestMessage)
    print(msg)
    sig = msgarr[1]
    signature = hmac.new(security_config.get_key(), msgarr[3].encode(), security_config.hashfunction).digest()
    assert(base64.b64encode(signature).decode() == sig)


def test_send_message_to_actor(server, security_config, target, sender):
    msg = TestMessage()
    send_message_to_actor(msg, target, sender, security_config)
    message = server.message_queue.get(timeout=30)
    message = json.loads(message)
    assert (type(message['sender']) is list)
    assert (message['sender'][0] == 'actorb')
    assert (message.get('message', None) is not None)
    clz = base64.b64decode(message['message'])
    clz = pickle.loads(clz)
    assert (type(clz) == TestMessage)
    assert (clz.test_str == 'Hello')
    assert (clz.test_int == 1)
    assert (type(clz.test_dict) is dict)
