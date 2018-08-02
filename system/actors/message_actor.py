"""
The base actor

@author aevans
"""
from multiprocessing import Queue

from actors.base_actor import BaseActor
from actors.modules.error import raise_not_handled_in_receipt
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


    def __handle_tell(self, message, sender):
        """
        Handle the tell request.

        :param message:  The message to handle
        :type message:  Tell
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass


    def __handle_ask(self, message, sender):
        """
        Handle and ask request

        :param message:  The actor message
        :type message:  Ask
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def __handle_broadcast(self, message, sender):
        """
        Send a broadcast to the target system

        :param message:  The actor message
        :type message:  Ask
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        pass

    def receive(self, message, sender):
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
        else:
            my_addr = self.config.myAddress
            raise_not_handled_in_receipt(message, my_addr)
