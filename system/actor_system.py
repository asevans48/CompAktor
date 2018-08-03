"""
The actor registry.

@author aevans
"""
from multiprocessing import Queue

from actors.base_actor import BaseActor
from actors.networked_actor import NetworkedActor
from messages.actor_maintenance import CreateActor, RegisterActor, UnRegisterGlobalActor, RegisterGlobalActor, \
    RemoveActor, SetActorStatus, StopActor, GetActorStatus
from networking.socket_server import SocketServerSecurity


class ActorSystem(BaseActor):

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
        self.is_convention_leader = False
        self.convention_leader = None
        self.__remove_systems = {}
        super(NetworkedActor, self).__init(
                self,
                actor_config,
                system_address,
                host,
                port,
                parent=[],
                max_threads=1000,
                signal_queue=Queue(),
                message_queue=Queue(),
                security=SocketServerSecurity())

    def __handle_create_actor(self, message, sender):
        """
        Handle a request to create an actor.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def __handle_register_global_actor(self, message, sender):
        """
        Handle a request to register a global actor.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def __handle_unregister_global_actor(self, message, sender):
        """
        Handle a request to unregister a global actor.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def __handle_remove_actor(self, message, sender):
        """
        Handle a request to remove an actor.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def __handle_register_actor(self, message, sender):
        """
        Handle a request to register an actor.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def __handle_set_actor_status(self, message, sender):
        """
        Handle a request to set a child actor status

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def __handle_stop_actor(self, message, sender):
        """
        Handle a request to stop the entire system.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def __handle_get_actor_status(self, message, sender):
        """
        Handle a request to get an actor status.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def receive(self, message, sender):
        """
        Handle the receipt of a message to the actor system.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        if type(message) is CreateActor:
            self.__handle_create_actor(message, sender)
        elif type(message) is RegisterActor:
            self.__handle_register_actor(message, sender)
        elif type(message) is UnRegisterGlobalActor:
            self.__handle_unregister_global_actor(message, sender)
        elif type(message) is RegisterGlobalActor:
            self.__handle_register_global_actor(message, sender)
        elif type(message) is RemoveActor:
            self.__handle_remove_actor(message, sender)
        elif type(message) is SetActorStatus:
            self.__handle_set_actor_status(message, sender)
        elif type(message) is StopActor:
            self.__handle_stop_actor(message, sender)
        elif type(message) is GetActorStatus:
            self.__handle_get_actor_status(message, sender)
        else:
            err_msg = 'Message Handle Not Implemented {} @ {}'.format(
                str(type(message)),
                self.config.myAddress
            )
            raise NotImplemented(err_msg)
