"""
The actor registry.

@author aevans
"""

from gevent.queue import Queue

from actors.base_actor import BaseActor, WorkPoolType
from actors.modules.error import raise_not_handled_in_receipt
from messages.actor_maintenance import RemoveActor, RegisterActor, CreateActor, RemoveChild, StopActor, ActorInf
from messages.system_maintenance import SetConventionLeader
from networking.socket_server import SocketServerSecurity, create_socket_server
from registry.registry import ActorRegistry, ActorStatus


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
        if self.config.convention_leader is None:
            self.is_convention_leader = True
            self.config.convention_leader = self.config.myAddress

    def __set_convention_leader(self, message, sender):
        """
        Sets the convention leader

        :param message:  The message to handler
        :type message:  SetConventionLeader
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        convention_address = message.actor_address
        my_addr = self.config.myAddress
        if convention_address.address is my_addr.address\
                and convention_address.host is my_addr.host\
                and convention_address.port is my_addr.port:
            self.is_convention_leader = True
        self.config.convention_leader = convention_address
        my_addr = self.config.myAddress
        message.target = message.sender
        message.sender = my_addr
        return message

    def __remove_actor(self, message, sender):
        """
        Handle the removal of an actor from the system.

        :param message:  The message to handle
        :type message:
        :param sender:
        """
        inf = None
        addr = message.actor_address
        if self.registery.get(addr.address, None):
            msg = StopActor(addr, self.config.myAddress)
            self.send(addr, msg)
            inf = self.registery.remove_actor(addr, terminate=False)
        my_addr = self.config.myAddress
        return ActorInf(inf, target=sender, sender=my_addr)


    def __register_actor(self, message, sender):
        """
        Register the actor.

        :param message:  The message to handle
        :type message:  RegisterActor
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def __create_actor(self, message, sender):
        """
        Handle the creation of an actor by message.

        :param message:  The message to handle
        :type message:  CreateActor
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        my_addr = self.config.myAddress
        actor_cfg = message.actor_config
        parent = [my_addr, ]
        actor = message.actor_class(actor_cfg, my_addr, parent)
        p = actor.start()
        actor_addr = actor.config.myAddress
        self.registery.add_actor(
            actor_addr,
            ActorStatus.RUNNING,
            actor.config.mailbox,
            p,
            parent)
        self.registery.add_child(my_addr, actor_addr)

    def __remove_child(self, message, sender):
        """
        Remove a child from an actor.  Assumes actor is stopped

        :param message:  The message to handle
        :type message:  RemoveChild
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        parent = message.parent_address
        child = message.child_address
        self.registery.remove_child(parent, child)

    def __handle_ask(self, message, sender):
        """
        Remove a child from an actor.  Assumes actor is stopped

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
        if type(message) is SetConventionLeader:
            self.__set_convention_leader(message, sender)
        elif type(message) is RemoveActor:
            self.__remove_actor(message, sender)
        elif type(message) is RegisterActor:
            self.__register_actor(message, sender)
        elif type(message) is CreateActor():
            self.__create_actor(message, sender)
        elif type(message) is RemoveChild:
            self.__remove_child(message, sender)
        else:
            my_addr = self.config.myAddress
            raise_not_handled_in_receipt(message, my_addr)


def create_actor(system, actor_config):
    """
    Create an actor on a system with the system as parent

    :param system:  The actor system
    :type system:  ActorAddress
    :param actor_config:  The actor config
    :type actor_config:  ActorConfig
    """
    pass


def tell(system, message):
    """
    Tell the system

    :param system:  The actor system
    :type system:  ActorAddress
    :param message:  The message to handle
    :type message:  BaseMessage
    """
    pass


def ask(system, message):
    """
    Ask for a response from the sytem. First response wins.

    :param system:  The system address
    :type system:  ActorAddress
    :param message:  The message to send
    :type message:  BaseMessage
    :return:  A new ask request with a wrapped response
    :rtype:  BaseMessage
    """
    pass


def broadcast(system, message):
    """
    Broadcasts a message through the entire system.

    :param system:  The actor system address
    :type system:  ActorAddress
    :param message:  The message to handle
    :type message:  BaseMessage
    """
    pass
