"""
Utilizes the same api as BaseActor but runs in a gevent loop instead of a new process.

Since gevent 1.3+, multi-processing queues work with a gevent thread.

Requires:  gevent 1.3+

@author aevans
"""
from gevent import Greenlet

from actors.base_actor import BaseActor


class GeventBaseActor(Greenlet, BaseActor):
    """
    Greenlet Based Actor Implementation
    """

    def __init__(self, actor_config, system_address, parent):
        """
        Constructor

        :param actor_config:  the actor configuration
        :type actor_config:  ActorConfig
        :param system_address:  The system address
        :type system_address:  ActorAddress
        :param parent:  The parent address chain for the actor
        :type parent:  list
        """
        Greenlet.__init__(self)
        BaseActor.__init__(self, actor_config, system_address, parent)

    def run(self):
        """
        Run the actor.  Continues to receive until a poisson pill is obtained
        """
        pass