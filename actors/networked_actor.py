"""
The networked actor used by the actor system to handle messages directly from the
gevent socket server.

@author aevans
"""
from multiprocessing import Process
from multiprocessing.queues import Queue

from actors.base_actor import BaseActor
from logging_handler import logging
from logging_handler.logging import package_error_message
from messages.utils import unpack_message
from networking.socket_server import SocketServerSecurity, SocketServer


class NetworkedActor(Process, BaseActor):

    def __init__(self,
                 actor_config,
                 system_address,
                 host,
                 port,
                 parent=[],
                 max_threads=1000,
                 signal_queue=Queue(),
                 message_queue=Queue(),
                 security=SocketServerSecurity()):
        """
        The constructor.

        :param actor_config:  the actor configuration
        :type actor_config:  ActorConfig
        :param system_address:  The system address
        :type system_address:  ActorAddress
        :param parent:  The parent address chain for the actor
        :type parent:  list
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
        self.socket_server =  SocketServer(host,
            port,
            max_threads,
            signal_queue,
            message_queue,
            security,
            message_handler=self.__handle_new_message)
        BaseActor.__init__(self, actor_config, system_address, parent)
        Process.__init__(self)

    def __handle_new_message(self, message):
        """
        Handle the receipt of a new message.  The networked receive

        :param message:  The message to handle
        :type message:  BaseMessage
        """
        try:
            message, sender = unpack_message(message)
            self._receive(message, sender)
        except Exception as e:
            message = package_error_message()
            logger = logging.get_logger()
            logging.log_error(logger, message)

    def __stop(self):
        """
        Stop this actor.  Requires stopping the underling  server.
        """
        self.socket_server.evt.set()

    def __handle_stop_actor(self, message, sender):
        """
        Overrides the base stop for good measure.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        self.__stop()

    def start(self):
        """
        Start the networked actor.
        :return:
        """
        self.socket_server.run()
