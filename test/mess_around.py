from random import randint

import gevent
import socket

from actors.address.addressing import ActorAddress
from messages.utils import get_object_from_message
from networking.socket_server import create_socket_server, SocketServerSecurity
from networking.utils import package_message
from test.modules.message import TestMessage

HOST = '127.0.0.1'
PORT = 12000


def send_to_server(inum):
    to = randint(0,1)
    gevent.sleep(to)
    addr = ActorAddress('test{}'.format(inum), HOST, PORT)
    sec = SocketServerSecurity()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
        msg = TestMessage()
        msg.test_int = inum
        message = package_message(msg, addr, sec)
        sock.send(message)
    finally:
        sock.close()


if __name__ == "__main__":
    pl = gevent.pool.Pool()
    server = create_socket_server(HOST, PORT, 2)
    print(server.is_alive())
    print(server.signal_queue.get(timeout=30))
    jobs = []
    for i in range(0, 10):
        jobs.append(gevent.spawn(send_to_server, i))
    gevent.joinall(jobs)
    for i in range(0, len(jobs)):
        msg = server.message_queue.get(timeout=30)
        print(get_object_from_message(msg).test_int)

