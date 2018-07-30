"""
The socket server.  This handles all remote communications.

@author aevans
"""

import atexit
import json
from threading import Thread

import gevent
from gevent import monkey
from gevent.queue import Queue
from gevent import signal
from gevent.event import Event
from gevent.pool import Pool
from gevent.server import StreamServer
monkey.patch_all()


class LengthStringRequiredException(object): pass


class ServerNotStartedException(Exception): pass


class ServerStarted(object): pass


class ServerStopped(object): pass


class SocketServer(Thread):
    """
    The socket server which extends Thread.
    """

    def __init__(self, host, port, max_threads=1000, signal_queue=Queue(), message_queue=Queue()):
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
        """
        Thread.__init__(self)
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
                    lstr = lstr.split(':::', maxsplit=1)
                    length = int(lstr[0].strip())
                    if len(lstr) > 0:
                        data += lstr[1].encode()
                except ValueError as e:
                    print('Received Non-Length String')
                    self.signal_queue.put(LengthStringRequiredException())
                except Exception as e:
                    self.signal_queue.put(e)
            gevent.sleep(0)
        out_message = {'data': data, 'address': address}
        self.message_queue.put(out_message)

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


def package_message(message):
    """
    Package a string message

    :param message:  The message to package
    :type message:  str
    :return:  a bytes like message with appropriate length
    :rtype:  bytes
    """
    if type(message) is not str:
        raise ValueError('String Message Required in package_message')
    return "{}:::{}".format(str(len(message)),message).encode()


def package_dict_as_json_string(message):
    """
    Package a dictionary as json string in a byte array.

    :param message:  The message to package
    :type message:  dict
    :return:  Json string in a byte array
    :rtype:  bytes
    """
    if type(message) is not dict:
        raise ValueError('Dict required in package_dict_as_json')
    msg = json.dumps(message)
    return "{}:::{}".format(str(len(message)), message).encode()


def create_socket_server(host, port, max_threads=1000):
    """
    Get the gevent thread containing the running server

    :param host:  The host address
    :type host:  str
    :param port:  The server port
    :type port:  int
    :param max_threads:  Maximum number of threads to start
    :type max_threads:  int
    :return:  threading.Thread
    """
    server = SocketServer(host, port, max_threads)
    server.start()
    return server


if __name__ == "__main__":
    host = '127.0.0.1'
    port = 12000
    socket_server = create_socket_server(host, port)
    socket_server.signal_queue.get(timeout=30)
    print('Starting Test')
    try:
        import socket
        for i in range(0, 100):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((host, port))
                msg = package_message('Hello')
                sock.send(msg)
            finally:
                sock.close()

        i = 0
        while socket_server.message_queue.empty() is False:
            i += 1
            message = socket_server.message_queue.get(timeout=30)
            assert (message is not None)
            assert (type(message) is dict)
            assert (message.get('data', None) is not None)
            assert (message['data'].decode() == 'Hello')
    finally:
        socket_server.stop_server()
        socket_server.signal_queue.get(timeout=30)
