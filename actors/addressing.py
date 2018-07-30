"""
Addressing for an actor system

@author aevans
"""

from atomos.atomic import AtomicLong


CURRENT_ADDRESS_NUM = AtomicLong()


class ActorAddress():
    """
    The ActorAddress object
    """
    def __init__(self, address, host='localhost'):
        """
        Constructor

        :param address:  The actor address string
        :type address:  str
        :param host:  The host string
        :type host:  str
        """
        self.address = address
        self.host = host

    def __repr__(self):
        return 'ActorAddress :: {} @ {}'.format(self.address, self.host)


def get_address():
    """
    Get the actor address

    :return:  ActorAddress
    """
    global CURRENT_ADDRESS_NUM
    lng = CURRENT_ADDRESS_NUM.get_and_add(1)
    return ActorAddress(lng)
