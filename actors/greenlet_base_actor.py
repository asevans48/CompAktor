"""
Utilizes the same api as BaseActor but runs in a gevent loop instead of a new process.

Since gevent 1.3+, multi-processing queues work with a gevent thread.

Requires:  gevent 1.3+

@author aevans
"""
from copy import deepcopy

import gevent
from gevent import Greenlet
from gevent.event import Event

from actors.base_actor import BaseActor
from logging_handler import logging
from logging_handler.logging import package_error_message
from messages.actor_maintenance import SetActorStatus, CreateActor, UnRegisterGlobalActor, RegisterGlobalActor, \
    RemoveActor, StopActor, GetActorStatus
from messages.poison import POISONPILL
from messages.routing import Broadcast, Tell, Ask, Forward
from networking.communication import send_message
from registry.registry import ActorStatus


class GreenletBaseActor(Greenlet, BaseActor):
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
        self.evt = Event()
        BaseActor.__init__(self, actor_config, system_address, parent)

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
            actor['actor_proc'].kill()
        except Exception as e:
            message = package_error_message(actor['address'])
            logger = logging.get_logger()
            logging.log_error(logger, message)

    def __stop(self):
        self.evt.set()
        self.running = False

    def run_loop(self, evt):
        """
        Run the gevent loop
        """
        self.running = True
        while self.running:
            message, sender = self.config.mailbox.get()
            if message == POISONPILL:
                self.running = False
                evt.set()
            else:
                try:
                    self._receive(message, sender)
                except Exception as e:
                    logger = logging.get_logger()
                    message = package_error_message(self.address)
                    logging.log_error(logger, message)
                gevent.sleep(0)
        if self.get_system_address():
            addr_object = SetActorStatus(
                self.config.myAddress, ActorStatus.STOPPED)
            send_message(
                addr_object,
                self.config.myAddress,
                self.get_system_address(),
                self.config.security_config)
        gevent.sleep(0)

    def run(self):
        """
        Run the actor.  Continues to receive until a poisson pill is obtained
        """
        self.run_loop(self.evt)
        self._post_stop()

    def _receive(self, message, sender):
        """
        The receieve hidden method helper.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The sender of the message
        :type sender:  ActorAdddress
        """
        if self._forward_as_needed(message, sender) is False:
            try:
                if type(message) is Broadcast:
                    self._handle_broadcast(message, sender)
                elif type(message) is Tell:
                    self._handle_tell(message, sender)
                elif type(message) is Ask:
                    return self._handle_ask(message, sender)
                elif type(message) is Forward:
                    return self._handle_forward(message, sender)
                elif type(message) is CreateActor:
                    self._handle_create_actor(message, sender)
                elif type(message) is UnRegisterGlobalActor:
                    self._handle_unregister_global_actor(message, sender)
                elif type(message) is RegisterGlobalActor:
                    self._handle_register_global_actor(message, sender)
                elif type(message) is RemoveActor:
                    self._handle_remove_actor(message, sender)
                elif type(message) is SetActorStatus:
                    self._handle_set_actor_status(message, sender)
                elif type(message) is StopActor:
                    self._handle_stop_actor(message, sender)
                elif type(message) is GetActorStatus:
                    self._handle_get_actor_status(message, sender)
                else:
                    return self.receive(message, sender)
            except Exception as e:
                logger = logging.get_logger()
                message = logging.package_error_message(self.address)
                logging.log_error(logger, message)
        elif self._global_actors.get(message.target.address, None):
            self._forward_message(message, sender)