"""
Implements base actor with each actor running in a new process.

@author aevans
"""
import pdb
from copy import deepcopy
from multiprocessing import Process

from actors.base_actor import BaseActor
from logging_handler import logging
from logging_handler.logging import package_error_message
from messages.poison import POISONPILL
from registry.registry import ActorStatus


class MultiprocessActorUtils(BaseActor):
    """
    Multi-processing based actor utilities.

    The constructor of a multiprocess actor should implement both this class
    and Process with Process being initialized first.  This actor can be combined
    with the NetworkedActorUtils to provide networking capabilities.
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
        BaseActor.__init__(self, actor_config, system_address, parent)

    def _post_start(self):
        """
        Can be overriden.  Is called after the process is started.
        """
        pass

    def __stop(self):
        """
        Stops the actor. Do not override
        """
        self.running = False


    def _handle_create_actor(self, message, sender):
        """
        Creates an actor and sets the actors parent to current actors
        parent with the current actor appended

        :param message:  The message to handle
        :type message:  CreateActor
        :param sender:  The sender
        :type sender:  ActorAddress
        """
        try:
            actor_class = message.actor_class
            actor_config = message.actor_config
            actor_parent = deepcopy(self._parent)
            actor_parent.append(self.config.myAddress.address)
            actor = actor_class(
                actor_config, self._system_address, actor_parent)
            actor.start()
            self._child_registry.add_actor(
                actor.config.myAddress,
                ActorStatus.RUNNING,
                actor.config.mailbox,
                actor_proc=actor,
                parent=actor_parent)
        except Exception as e:
            logger = logging.get_logger()
            message = package_error_message(self.address)
            logging.log_error(logger, message)

    def _handle_stop_child(self, actor):
        """
        Stop the child actor
        :param actor:  The actor to stop
        :type actor:  BaseActor
        """
        try:
            p = actor['actor_proc']
            p.terminate()
            p.join()
        except Exception as e:
            message = package_error_message(actor['address'])
            logger = logging.get_logger()
            logging.log_error(logger, message)

    def _loop(self):
        """"
        Run the actor.  Continues to receive until a poisson pill is obtained
        """
        self.running = True
        self._post_start()
        while self.running:
            message, sender = self.config.mailbox.get()
            if type(message) is POISONPILL:
                self.running = False
            else:
                try:
                    self._receive(message, sender)
                except Exception as e:
                    logger = logging.get_logger()
                    message = package_error_message(self.address)
                    logging.log_error(logger, message)
        self._post_stop()
