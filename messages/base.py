


class BaseMessage(object):
    """
    Base inheritable message with common variables
    """

    def __init__(self, target, sender=None):
        """
        Constructor

        :param target:  The target address
        :type target:  ActorAddress
        :param sender:  The sender address
        :type sender:  ActorAddress
        """
        self.target = target
        self.sender = sender