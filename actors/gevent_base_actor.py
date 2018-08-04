"""
Utilizes the same api as BaseActor but runs in a gevent loop instead of a new process.

Since gevent 1.3+, multi-processing queues work with a gevent thread.

Requires:  gevent 1.3+

@author aevans
"""
import gevent
from gevent import Greenlet

from actors.base_actor import BaseActor
from logging_handler import logging
from logging_handler.logging import package_error_message
from messages.actor_maintenance import SetActorStatus
from messages.poison import POISONPILL
from networking.communication import send_message
from registry.registry import ActorStatus


class GeventBaseActor(Greenlet, BaseActor):
    """
    Greenlet Based Actor Implementation
    """

    def __init__(self, actor_config, system_address, parent=[]):
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
        self.running = True
        while self.running:
            message, sender = self.inbox.get()
            if type(message) is POISONPILL:
                self.running = False
            else:
                try:
                    self._receive(message, sender)
                except Exception as e:
                    logger = logging.get_logger()
                    message = package_error_message(self.address)
                    logging.log_error(logger, message)
                gevent.sleep(0)
        addr_object = SetActorStatus(self.myAddress, ActorStatus.STOPPED)
        send_message(
            addr_object,
            self.config.myAddress,
            self.get_system_address(),
            self.config.security_config)