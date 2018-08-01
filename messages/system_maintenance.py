"""
Messages for system wide maintenance

@author aevans
"""
from messages.base import BaseMessage


class SetConventionLeader(BaseMessage):

    def __init__(self, actor_address, target, sender):
        """
        Constructor

        :param actor_address:  The actor address
        :type actor_address:  ActorAddress
        :param target:  Target actor address
        :type target:  ActorAddress
        :param sender:  Message sender
        :type sender:  ActorAddress
        """
        super(SetConventionLeader, self).__init__(target, sender)
        self.actor_address = actor_address
