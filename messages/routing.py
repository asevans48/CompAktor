"""
Python does not have threads so each actor has its child registry.  Routing
means either forwarding to a child or routing.

@author aevans
"""
from messages.base import BaseMessage


class Ask(BaseMessage):
    """
    An ask message
    """

    def __init__(self, message, target, sender):
        """
        Constructor

        :param message:  The message to send
        :type message:  BaseMessage
        :param target:  The target actor
        :type target:  ActorAddress
        :param sender:  The sender
        :type sender:  ActorAddress
        """
        super(Ask, self).__init__(target, sender)
        self.message = message


class Tell(BaseMessage):
    """
    A tell message
    """

    def __init__(self, message, target, sender):
        """
        Constructor

        :param message:  The message to send
        :type message:  BaseMessage
        :param target:  The target actor
        :type target:  ActorAddress
        :param sender:  The sender
        :type sender:  ActorAddress
        """
        super(Ask, self).__init__(target, sender)
        self.message = message


class Broadcast(BaseMessage):
    """
    A broadcast message
    """

    def __init__(self, message, target, sender):
        """
        Constructor

        :param message:  The message to broadcast
        :type message:  BaseMessage
        :param target:  The target actor
        :type target:  ActorAddress
        :param sender:  The sender
        :type sender:  ActorAddress
        """
        super(Broadcast, self).__init__(target, sender)
        self.message = message


class Forward(BaseMessage):
    """
    Forward to a child
    """

    def __init__(self, message, address_chain, target, sender):
        """
        Constructor

        :param address_chain:  The address chain to follow
        :type address_chain:  list
        :param message:  The message to forward
        :type message:  BaseMessage
        :param target:  The target actor
        :type target:  ActorAddress
        :param sender:  The sender
        :type sender:  ActorAddress
        """
        super(Forward, self).__init__(target, sender)
        self.message = message
        self.address_chain = address_chain
