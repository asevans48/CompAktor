"""
The actor registry.

@author aevans
"""

from gevent.queue import Queue

from networking.socket_server import SocketServerSecurity, create_socket_server
from registry.registry import ActorRegistry


class SystemConfig(object):
    host = None
    port = 0
    connect_timeout = 2


class ActorSystem(object):

    def __init__(self, system_config, security_config=SocketServerSecurity()):
        """
        Constructor

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

    def find_actor(self, target, default=None):
        """
        Find an actor.

        :param target:  The target actor
        :type target:  ActorAddress
        :param default:  The default value to return
        :type default:  object
        :return: The actor information or default
        :rtype:  dict
        """
        return self.registery.get_actor(target)

    def create_actor(self, actor_class, parent=None):
        """
        Create an actor
        :param actor_class:  The class of the actor
        :type actor_class:  object
        :param parent:  The parent actor if applicable
        :return:  The started actor process with informations
        :rtype
        """
        pass


    def stop_actor(self):
        pass

    def tell(self, message, target):
        pass

    def ask(self, message, target):
        pass
