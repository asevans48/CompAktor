"""
The actor registry.

@author aevans
"""
from multiprocessing import Queue

from actors.base_actor import BaseActor
from actors.networked_actor import NetworkedActor
from messages.actor_maintenance import CreateActor, RegisterActor, UnRegisterGlobalActor, RegisterGlobalActor, \
    RemoveActor, SetActorStatus, StopActor, GetActorStatus
from messages.routing import Forward
from messages.system_maintenance import SetConventionLeader
from networking.socket_server import SocketServerSecurity
from registry.registry import ActorRegistry, ActorStatus


class ActorSystem(NetworkedActor):

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
        self.__child_registry = ActorRegistry()
        self.is_convention_leader = False
        self.convention_leader = None
        self.__remove_systems = {}
        super(NetworkedActor, self).__init(
                self,
                actor_config,
                system_address,
                host,
                port,
                parent,
                max_threads,
                signal_queue,
                message_queue,
                security)

    def __handle_set_convention_leader(self, message, sender):
        """
        Handle setting of a convention leader.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        self.convention_leader = message.actor_address
        my_addr = self.config.myAddress
        if my_addr.host is message.host and my_addr.port is message.port:
            self.is_convention_leader = True

    def receive(self, message, sender):
        """
        Handle the receipt of a message to the actor system.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        if type(message) is SetConventionLeader:
            self.__handle_set_convention_leader(message, sender)
        else:
            err_msg = 'Message Handle Not Implemented {} @ {}'.format(
                str(type(message)),
                self.config.myAddress
            )
            raise NotImplemented(err_msg)
