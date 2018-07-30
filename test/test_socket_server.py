
import pytest
import socket

from django.core.serializers import json

from networking.socket_server import start_socket_server, ServerStarted, \
    package_dict_as_json_string, package_message, ServerStopped

HOST = '127.0.0.1'
PORT = 12000


@pytest.fixture
def socket_server():
    server = start_socket_server(HOST, PORT, 2)
    return server


@pytest.mark.order1
def test_package_dict():
    jdict = {'a': 1}
    dstr = package_dict_as_json_string(jdict)
    assert(dstr is not None)
    assert(type(dstr) is bytes)
    dstr.decode()
    darr = dstr.split(':::')
    assert(len(darr) is 2)
    jdict = json.loads(darr[1])
    assert(jdict is not None)
    assert(type(jdict) is dict)
    assert(jdict.get('a', 0) is 1)


@pytest.mark.order2
def test_package_string():
    msg = 'hello'
    msg = package_message(msg)
    assert(msg is not None)
    assert(type(msg) is bytes)
    msg = msg.decode()
    marr = msg.split(':::')
    assert(len(marr) is 2)
    assert(marr[1] == 'hello')
    assert(int(marr[0].strip()) is len('hello'))


@pytest.mark.order3
def test_server_start(socket_server):
    msg = socket_server.signal_queue.get(timeout=30)
    assert(type(msg) == ServerStarted)


@pytest.mark.order4
def test_simple_string_server_comms(socket_server):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    msg = package_message('hello')
    sock.send(msg)
    message = socket_server.message_queue.get(timeout=30)
    assert(message is not None)
    assert(type(message) is dict)
    assert(message.get('data', None) is not None)
    assert(message['data'] == 'hello')


@pytest.mark.order5
def test_whitespace_and_chars_string_server_comms(socket_server):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    msg = package_message(' hello @!$*#( everybody ')
    sock.send(msg)
    message = socket_server.message_queue.get(timeout=30)
    assert (message is not None)
    assert (type(message) is dict)
    assert (message.get('data', None) is not None)
    assert (message['data'] == ' hello @!$*#( everybody ')


@pytest.mark.order6
def test_hundred_string_server_comms(socket_server):
    for i in range(0, 100):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        msg = package_message(msg)
        sock.send(msg)
    message = socket_server.message_queue.get(timeout=30)
    assert (message is not None)
    assert (type(message) is dict)
    assert (message.get('data', None) is not None)
    assert (message['data'] == ' hello @!$*#( everybody ')


@pytest.mark.order7
def test_json_server_comms(socket_server):
    pass


@pytest.mark.order8
def test_stop_server(socket_server):
    socket_server.stop()
    msg = socket_server.signal_queue.get(timeout=30)
    assert(type(msg) == ServerStopped)
