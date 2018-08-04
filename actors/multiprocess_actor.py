"""
Implements base actor with each actor running in a new process.

@author aevans
"""
import gevent
from multiprocessing import Process

from actors.base_actor import BaseActor
from logging_handler import logging
from logging_handler.logging import package_error_message
from messages.actor_maintenance import SetActorStatus
from messages.poison import POISONPILL
from networking.communication import send_message
from registry.registry import ActorStatus


class MultiprocessActor(Process, BaseActor):
    """
    Multi-processing based actor implementation
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
        Process.__init__(self)
        BaseActor.__init__(self, actor_config, system_address, parent)

    def start(self):
        """"
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
        addr_object = SetActorStatus(self.myAddress, ActorStatus.STOPPED)
        send_message(
            addr_object,
            self.config.myAddress,
            self.get_system_address(),
            self.config.security_config)