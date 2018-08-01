"""
The socket server.  This handles all remote communications.

@author aevans
"""

import atexit
import base64
import hashlib
import hmac
import json
from threading import Thread

import gevent
from gevent import lock
from gevent import monkey
from gevent import signal
from gevent.event import Event
from gevent.pool import Pool
from gevent.queue import Queue
from gevent.server import StreamServer

from logging_handler import logging
from messages.utils import get_message_sender, get_object_from_message

monkey.patch_all()


class LengthStringRequiredException(object):
    pass


class ServerNotStartedException(Exception):
    pass


class ServerStarted(object):
    pass


class ServerStopped(object):
    pass


class SocketServerSecurity(object):

    def __init__(self):
        self.__key = bytes("ctv4eys984cavpavt5snldbkrw3".encode('utf-8'))
        self.hashfunction = hashlib.sha256
        self.__hashsize = 256 / 8
        self.__magic = 'sendreceive'
        self.buffer = 8192

    def get_key(self):
        return self.__key

    def get_hash_size(self):
        return self.__hashsize

    def get_magic(self):
        return self.__magic


class SocketServer(Thread):
    """
    The socket server which extends Thread.
    """

    def __init__(
            self,
            host,
            port,
            max_threads=1000,
            signal_queue=Queue(),
            message_queue=Queue(),
            security=SocketServerSecurity()):
        """
        The constructor.

        :param host:  The host string
        :type host:  str
        :param port:  The port for the server
        :type port:  int
        :param max_threads:  Maximum number of connection threads default is 1000
        :type max_threads:  int
        :param signal_queue:  The underlying signal queue which has a default
        :type signal_queue:  gevent.queue.Queue
        :param message_queue:  The underlying message queue
        :type mssage_queue:  gevent.queue.Queue
        :param registry:  The registry for forwarding a message
        :type registry:  dict
        """
        Thread.__init__(self)
        self.__security = security
        self.is_running = False
        self.__server = None
        self.host = host
        self.port = port
        self.evt = Event()
        self.signal_queue = signal_queue
        self.message_queue = message_queue
        self._socket_timeout = 10
        self.__max_threads = max_threads

    @property
    def socket_timeout(self):
        return self._socket_timeout

    @socket_timeout.setter
    def socket_timeout(self, timeout):
        self._socket_timeout = timeout

    def handle_socket(self, socket, address):
        """
        Handle an initial socket. Expects length as first part of string.
        :param socket:  The socket to handle
        :type socket:  gevent.socket
        """
        length = -1
        data = b''
        socket.settimeout(self._socket_timeout)
        accepted = False
        signature = None
        while len(data) - length > 0:
            if length > 0 and len(data) - length > 0:
                new_data = socket.recv(1024)
                if new_data:
                    data += new_data
                else:
                    break
            elif length == -1:
                ldata = socket.recv(1024)
                if not ldata:
                    self.signal_queue.put(LengthStringRequiredException())
                    break
                try:
                    lstr = ldata.decode('utf-8')
                    lstr = lstr.split(':::', maxsplit=4)
                    if len(lstr) > 2:
                        mpart = lstr[0]
                        signature = lstr[1]
                        length = int(lstr[2])
                        if mpart != self.__security.get_magic():
                            break
                        accepted = True
                    else:
                        break
                    if len(lstr) > 3:
                        data += lstr[3].encode()
                except ValueError as e:
                    logging.log_error('Received Non-Length String')
                    self.signal_queue.put(LengthStringRequiredException())
                except Exception as e:
                    self.signal_queue.put(e)
            gevent.sleep(0)
        if accepted and signature:
            sig2 = hmac.new(self.__security.get_key(), data, self.__security.hashfunction).digest()
            sig2 = base64.b64encode(sig2).decode()
            if sig2 != signature:
                accepted = False
        else:
            accepted = False
        omessage = json.loads(data.decode())
        self.message_queue.put(omessage)

    def stop_server(self):
        """
        Stop a running server.  Sets the event
        """
        try:
            self.evt.set()
        except Exception as e:
            self.signal_queue.put(e)

    def _stop_server(self, timeout=None):
        """
        Stop a running server
        :param timeout:  Time to wait for the server to stop
        :type timeout:  int
        :return:  an appropriate return message
        :rtype:  ServerStopped
        """
        failed = False
        if self.is_running:
            try:
                if timeout:
                    self.__server.stop(timeout)
                else:
                    self.__server.stop()
                self.is_running = False
                failed = True
            except Exception as e:
                self.signal_queue.put(e)
        if failed is False:
            self.signal_queue.put(ServerStopped())

    def _setup_termination(self):
        """
        Setup handlers for termination
        """
        gevent.signal(signal.SIGQUIT, self.evt.set)
        gevent.signal(signal.SIGTERM, self.evt.set)
        gevent.signal(signal.SIGINT, self.evt.set)
        atexit.register(self._stop_server)

    def run(self):
        """
        Create and start the StreamServer
        :param host:  The server address
        :type host:  str
        :param port:  The server port
        :type port:  int
        """
        try:
            def handle_connect(socket, address):
                self.handle_socket(socket, address)
            pool = Pool(self.__max_threads)
            self.__server = StreamServer((self.host, self.port), handle_connect, spawn=pool)
            self.__server.start()
            self._setup_termination()
            self.is_running = True
            self.signal_queue.put(ServerStarted())
            self.evt.wait()
            self._stop_server(10)
        except Exception as e:
            self.signal_queue.put(e)
        finally:
            self.signal_queue.put(ServerStopped())


def create_socket_server(host, port, max_threads=1000, security_config=SocketServerSecurity()):
    """
    Get the gevent thread containing the running server

    :param host:  The host address
    :type host:  str
    :param port:  The server port
    :type port:  int
    :param max_threads:  Maximum number of threads to start
    :type max_threads:  int
    :param security_config:  The security configuration
    :type security_config:  SocketServerSecurity
    :return:  threading.Thread
    """
    server = SocketServer(host, port, max_threads, security=security_config)
    server.start()
    return server
