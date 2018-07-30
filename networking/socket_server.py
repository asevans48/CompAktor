"""
The socket server.  This handles all remote communications.

@author aevans
"""
import atexit
from multiprocessing import Queue
from threading import Thread

import gevent
from gevent import monkey
from gevent import signal
from gevent.event import Event
from gevent.pool import Pool
from gevent.server import StreamServer

MAX_THREADS = 10000
monkey.patch_all()


class ServerNotStartedException(Exception): pass


class ServerStarted(object): pass


class ServerStopped(object): pass


class SocketServer(Thread):
    """
    The socket server which extends Thread.
    """

    def __init__(self, host, port, signal_queue=Queue(), message_queue=Queue()):
        """
        The constructor.

        :param host:  The host string
        :type host:  str
        :param port:  The port for the server
        :type port:  int
        """
        Thread.__init__(self)
        self.__server = None
        self.host = host
        self.port = port
        self.evt = Event()
        self.signal_queue = signal_queue
        self.message_queue = message_queue

    def handle_socket(self, socket):
        chunk_size = 512 * 1024  # how many bytes we want to process each loop
        while True:
            data = ''
            while len(data) < chunk_size:
                data += socket.recv(chunk_size - len(data))
            if not data:
                break
            gevent.sleep(5)
        socket.close()

    def stop_server(self):
        try:
            self.evt.set()
            self.signal_queue.put(ServerStopped())
        except Exception as e:
            self.signal_queue.put(e)

    def run(self):
        """
        Create and start the StreamServer
        :param host:  The server address
        :type host:  str
        :param port:  The server port
        :type port:  int
        """
        evt = Event()
        try:
            print("Creating Server")
            def handle_connect(socket, address):
                self.handle_socket(socket)
            pool = Pool(MAX_THREADS)
            self.__server = StreamServer((self.host, self.port), handle_connect, spawn=pool)
            self.__server.start()
            gevent.signal(signal.SIGQUIT, self.evt.set)
            gevent.signal(signal.SIGTERM, self.evt.set)
            gevent.signal(signal.SIGINT, self.evt.set)
            self.signal_queue.put(ServerStarted())
            atexit.register(self.__server.stop)
            self.evt.wait()
            self.__server.stop(10)
        except Exception as e:
            self.signal_queue.put(e)
        finally:
            self.signal_queue.put(ServerStopped())

def start_socket_server(host, port):
    """
    Get the gevent thread containing the running server

    :param host:  The host address
    :type host:  str
    :param port:  The server port
    :type port:  int
    :return:  threading.Thread
    """
    sthread = SocketServer(host, port)
    sthread.start()
    return sthread
