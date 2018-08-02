"""
The base actor

@author aevans
"""
from multiprocessing import Queue

from actors.base_actor import BaseActor
from actors.modules.error import raise_not_handled_in_receipt
from messages.actor_maintenance import CreateActor
from messages.base import BaseMessage
from messages.routing import Ask, Tell, Broadcast


class MessageActor(BaseActor):

    def __init__(self, config, system_address):
        """
        Constructor

        :param config:  The actor config
        :type config:  ActorConfig
        :param system_address:  The system address
        :type system_address:  ActorAddress
        """
        self.entry_queue = Queue()
        super(MessageActor, self).__init__(config, system_address)

    def __handle_broadcast(self, message, sender):
        """
        Send a broadcast to the target system

        :param message:  The actor message
        :type message:  Ask
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        target = self.get_system_address()
        message.target = target
        self.send(target, message)

    def __handle_create_actor(self, message, sender):
        """
        Handle an actor creation request
        :param message:  The message to handle
        :type message:  CreateActor
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        target = self.get_system_address()
        message.target = target
        self.send(target, message)

    def __receive(self, message, sender):
        """
        Handle message receipt.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        if type(message) is Ask:
            self.__handle_ask(message, sender)
        elif type(message) is Tell:
            self.__handle_tell(message, sender)
        elif type(message) is Broadcast:
            self.__handle_broadcast(message, sender)
        elif type(message) is CreateActor:
            self.__handle_create_actor
        else:
            my_addr = self.config.myAddress
            raise_not_handled_in_receipt(message, my_addr)
