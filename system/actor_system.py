"""
The actor registry.

@author aevans
"""

from gevent.queue import Queue

from actors.base_actor import BaseActor
from networking.socket_server import SocketServerSecurity, create_socket_server
from registry.registry import ActorRegistry


class SystemConfig(object):
    """
    System configuration
    """
    host = None
    port = 0
    connect_timeout = 2


class ActorSystem(BaseActor):

    def __init__(self, actor_config, system_config, security_config=SocketServerSecurity()):
        """
        Constructor

        :param actor_config:  The system is a separate process running on an actor
        :type actor_config:  ActorConfig
        :param system_config:  General system configuration
        :type system_config:  SystemConfig
        :param security_config:  The security configuration
        :type security_config:  SocketServerSecurity
        """
        self.convention_leader = None
        self.is_convention_leader = False
        self.server = None
        self.remote_systems = {}
        if system_config.host and system_config.port > 2000:
            self.server = create_socket_server(
                system_config.host,
                system_config.port,
                security_config=security_config)
            self.server.signal_queue.get(timeout=10)
        self.registery = ActorRegistry()
        self.message_queue = Queue()
        super(ActorSystem, self).__init__(actor_config, self.message_queue)

    def create_actor(self, actor_class):
        """
        Create an actor on this system and place in the registry.  The
        system becomes the parent.

        :param actor_class:  The class of the actor
        :type actor_class:  object
        :param parent:  The parent actor if applicable
        :return:  The started actor process with informations
        :rtype
        """
        pass

    def receceive(self):
        pass


def tell(system, message, target):
    pass


def ask(system, message, target):
    pass
