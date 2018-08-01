"""
The actor registry.

@author aevans
"""

from gevent.queue import Queue

from actors.base_actor import BaseActor, WorkPoolType
from networking.socket_server import SocketServerSecurity, create_socket_server
from registry.registry import ActorRegistry


class ActorSystem(BaseActor):

    def __init__(self, actor_config):
        """
        Constructor

        :param actor_config:  The system is a separate process running on an actor
        :type actor_config:  ActorConfig
        :param security_config:  The security configuration
        :type security_config:  SocketServerSecurity
        """
        actor_config.work_pool_type = WorkPoolType.NO_POOL
        super(ActorSystem, self).__init__(actor_config, self.message_queue)
        self.convention_leader = None
        self.is_convention_leader = False
        self.server = None
        self.remote_systems = {}
        if self.config.host and self.config.port > 2000:
            self.server = create_socket_server(
                self.config.host,
                self.config.port,
                security_config=self.config.security_config)
            self.server.signal_queue.get(timeout=10)
        self.registery = ActorRegistry()
        self.message_queue = Queue()

    def set_convention_leader(self, message, sender):
        """
        Sets the convention leader

        :param message:  The message to handler
        :type message:  SetConventionLeader
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def remove_actor(self, message, sender):
        """
        Handle the removal of an actor from the system.

        :param message:  The message to handle
        :type message:
        :param sender:
        """
        pass


    def register_actor(self, message, sender):
        """
        Register the actor.

        :param message:  The message to handle
        :type message:  RegisterActor
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def create_actor(self, message, sender):
        """
        Handle the creation of an actor by message.

        :param message:  The message to handle
        :type message:  CreateActor
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def remove_child(self, message, sender):
        """
        Remove a child from an actor.

        :param message:  The message to handle
        :type message:  RemoveChild
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def receive(self, message, sender):
        """
        Handle the receipt of a message.

        :param message:  The received message
        :type message:  object
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass


def tell(system, message, target):
    pass


def ask(system, message, target):
    pass
