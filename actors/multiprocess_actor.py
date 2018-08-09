"""
Implements base actor with each actor running in a new process.

@author aevans
"""
import atexit
import multiprocessing
from copy import deepcopy
from multiprocessing import Process, Manager

from actors.address.addressing import get_address
from actors.base_actor import BaseActor, WorkPoolType
from logging_handler import logging
from logging_handler.logging import package_error_message
from messages.actor_maintenance import StopActor, ActorState, ActorStatusResponse, CreateActor, UnRegisterGlobalActor, \
    RegisterGlobalActor, RemoveActor, SetActorStatus, GetActorStatus
from messages.base import BaseMessage
from messages.poison import POISONPILL
from messages.routing import Forward, Ask, ReturnMessage, Broadcast, Tell
from networking.communication import send_message
from networking.utils import package_message
from pools.asyncio_work_pool import AsyncioWorkPool
from pools.greenlet_pool import GreenletPool
from pools.multiproc_pool import MultiProcPool
from registry.registry import ActorStatus, ActorRegistry


class MultiprocessBaseActor(object):
    """
    Multi-processing based actor utilities.

    The constructor of a multiprocess actor should implement both this class
    and Process with Process being initialized first.  This actor can be combined
    with the NetworkedActorUtils to provide networking capabilities.
    """

    def __init__(self, actor_config, system_address, parent=[]):
        """
        Constructor

        :param actor_config:  The actor configuration
        :type actor_config:  ActorConfig
        :param system_address:  The actor system address
        :type system_address:  ActorAddress
        :param parent:  The parent actor if applicable
        :type parent:  ActorAddress
        """
        self.__manager = Manager()
        self.state = ActorStatus.SETUP
        self._global_actors = {}
        self.config = actor_config
        if self.config.mailbox is None:
            mbox = self.__manager.Queue()
            self.config.mailbox = mbox
        self._system_address = system_address
        self._parent = parent
        self.address = self.config.myAddress
        if self.config.myAddress is None:
            self.address = get_address('', 0)
            if self._system_address:
                self.address = get_address(system_address.host, system_address.port)
            self.config.myAddress = self.address
        else:
            self.address = self.config.myAddress
        work_pool_type = self.config.work_pool_type
        max_workers = self.config.max_workers
        self.__work_pool = None
        self._child_registry = ActorRegistry()
        if work_pool_type == WorkPoolType.GREENLET:
            self.__work_pool = GreenletPool(
                max_workers=max_workers)
        elif work_pool_type == WorkPoolType.ASNYCIO:
            self.__work_pool = AsyncioWorkPool(
                max_workers=max_workers)
        elif work_pool_type == WorkPoolType.PROCESS:
            self.__work_pool = MultiProcPool(
                max_workers=max_workers)
        atexit.register(self._cleanup)
        Process.__init__(self)

    def get_system_address(self):
        """
        Gets the system address
        :return:  The system address
        :rtype:  ActorAddress
        """
        return self._system_address

    def _get_parent(self):
        """
        Get the parent address.

        :return:  The parent address
        :rtype:  ActorAddress
        """
        return self._parent

    def _post_stop(self):
        """
        Called after stop but before cleanup.
        """
        pass

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

    def __stop_children(self):
        try:
            child_keys = self._child_registry.get_keys()
            if len(child_keys):
                for key in child_keys:
                    child = self._child_registry.get_actor(key)
                    try:
                        message = StopActor(child['address'], self.config.myAddress)
                        addr = self.config.myAddress
                        child['mailbox'].put_nowait((message, addr))
                    except Exception as e:
                        logger = logging.get_logger()
                        message = package_error_message(self.address)
                        logging.log_error(logger, message)
                    finally:
                        self._handle_stop_child(child)
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message(self.address)
            logging.log_error(logger, message)
            logging.log_error(logger, message)

    def _cleanup(self):
        """
        Called at exit to cause the actor to perform cleanup work.
        """
        try:
            if self.__work_pool:
                self.__work_pool.close()
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message(self.address)
            logging.log_error(logger, message)
        try:
            if type(self.config.mailbox) is multiprocessing.Queue:
                self.config.mailbox.close()
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message(self.address)
            logging.log_error(logger, message)
        try:
            self.__manager.shutdown()
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message(self.address)
            logging.log_error(logger, message)

    def _setup(self):
        """
        Called at actor start to setup the actor.
        """
        logger = logging.get_logger()
        if self.config.log_config:
            logging.add_logstash_handler(logger, self.config.log_config)
        if self.config.apm_config:
            logging.setup_apm(self.config.apm_config)
        logging.add_console_handler(logger, self.config.log_config)

    def _forward_message(self, message, sender):
        """
        Forward a message based on the given parent chain.

        :param message:  The message to handle
        :type message: BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        message_parent = message.target.parent
        path = deepcopy(message_parent)
        if self.config.myAddress.address in path:
            path.append(message.target.address)
            idx = path.index(self.config.myAddress.address)
            next = path[idx + 1]
            if self._child_registry.get_actor(next):
                child = self._child_registry[next]
                message = Forward(message, path, message.target, sender)
                child.mailbox.put_nowait(message, message.target, self.config.myAddress)

    def _forward_as_needed(self, message, sender):
        """
        Forward a message if necessary.  Returns forwarded if address
        does not exist on this tree path.

        :param message:  The message to send
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        :return:  whether the message was forwarded
        :rtype:  boolean
        """
        if message.target.__eq__(self.address):
            return False
        target = message.target
        if self._child_registry.get_actor(target, None):
            child = self._child_registry.get_actor(target.address)
            child['mailbox'].put_nowait((message, sender))
            return True
        elif target.__eq__(self.address) is False\
                and target.host is not None\
                and target.port != 0:
            self.send(target, message)
            return True
        return False

    def send(self, target, message):
        """
        Send the a message to another actor.

        :param target:  The target actor address
        :type target:  ActorAddress
        :param message:  The pickle-able message to send
        :type message:  object
        :return:  The success status of send
        :rtype:  boolean
        """
        success = False
        forwarded = self._forward_as_needed(message, self.address)
        if forwarded is False:
            msg = package_message(
                message,
                self.address,
                self.config.server_security,
                target)
            if target and target.host and target.port:
                try:
                    sender = self.config.myAddress
                    send_message(msg, sender, target, self.config.security_config)
                    success = True
                except Exception as e:
                    message = logging.package_error_message(self.address)
                    logger = logging.get_logger()
                    logging.log_error(logger, message)
            return success
        else:
            return forwarded

    def __create_actor(self, message, sender):
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
            p = actor.start()
            self._child_registry.add_actor(
                actor.config.myAddress,
                ActorStatus.RUNNING,
                actor.config.mailbox,
                actor_proc=p,
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
            if p:
                p.terminate()
                p.join()
        except Exception as e:
            message = package_error_message(actor['address'])
            logger = logging.get_logger()
            logging.log_error(logger, message)

    def _handle_get_actor_state(self, message, sender):
        """
        Gets the actor state

        :param message:  The message to handle
        :type message:  GetActorState
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        msg = ActorState(self.state, sender, message.target)
        self.send(message.target, msg)

    def _handle_broadcast(self, message, sender):
        """
        The message to broadcast

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        fwd = self._forward_as_needed(message, sender)
        if fwd is False:
            for child in self._child_registry.get_keys():
                try:
                    inf = self._child_registry.get_actor(child)
                    mailbox = inf['mailbox']
                    mailbox.put_nowait((message, sender))
                except Exception as e:
                    logger = logging.get_logger()
                    message = "Failed to Broadcast {} --> {}".format(
                        self.config.myAddress,
                        child
                    )
                    logging.log_error(logger, message)
            msg = message.message
            self.receive(msg, sender)
        elif self._global_actors.get(message.target.address, None):
            self._forward_message(message, sender)

    def _handle_ask(self, message, sender):
        """
        The message to ask

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        if self._forward_as_needed(message, sender) is False:
            msg = message.message
            rval = self.receive(msg)
            if rval and issubclass(rval, BaseMessage):
                omessage = Ask(rval)
            else:
                my_addr = self.config.myAddress
                omessage = ReturnMessage(rval, sender, my_addr)
            self.send(sender, omessage)
        elif self._global_actors.get(message.target.address, None):
            self._forward_message(message, sender)

    def _handle_tell(self, message, sender):
        """
        The message to tell

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        if self._forward_as_needed(message, sender) is False:
            self.receive(message.message, sender)
        elif self._global_actors.get(message.target.address, None):
            self._forward_message(message, sender)

    def _handle_forward(self, message, sender):
        msg = message.message
        chain = message.address_chain
        if chain and len(chain) > 0:
            addr = chain.pop(0)
            if self._child_registry.has_actor(addr):
                target_addr = self._child_registry.get_actor(addr)
                target_addr = target_addr['address']
                new_fwd = Forward(msg, chain, target_addr, sender)
                mbox = self._child_registry.get_actor(addr)['mailbox']
                mbox.put_no_wait((new_fwd, target_addr))
            elif addr == self.address.address:
                self.receive(msg, sender)
        else:
            self.receive(msg, message.sender)

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
            mbox = actor_config.mailbox
            if mbox is None:
                mbox = self.__manager.Queue()
                actor_config.mailbox = mbox
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

    def _handle_register_global_actor(self, message, sender):
        """
        Handle a request to register a global actor.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        actor_address = message.actor_address.address
        self._global_actors[actor_address] = message.actor_address

    def _handle_unregister_global_actor(self, message, sender):
        """
        Handle a request to unregister a global actor.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        actor_address = message.actor_address.address
        self._global_actors.pop(actor_address)

    def _handle_remove_actor(self, message, sender):
        """
        Handle a request to remove an actor.  Force it to stop.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        actor_address = message.actor_address.address
        if self._child_registry.get_actor(message.actor_address, None):
            msg = StopActor(message.actor_address, sender)
            child = self._child_registry.get_actor(message.actor_address)
            child['mailbox'].put_nowait((msg, sender))
            self._child_registry.remove_actor(message.actor_address)

    def _handle_set_actor_status(self, message, sender):
        """
        Handle a request to set a child actor status

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        actor_addr = message.actor_address
        actor_status = message.status
        self._child_registry.set_actor_status(actor_addr, actor_status)

    def _handle_stop_actor(self, message, sender):
        """
        Handle a request to stop the entire system.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        self.__stop()
        self.state = ActorStatus.STOPPED

    def _handle_get_actor_status(self, message, sender):
        """
        Handle a request to get an actor status.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        actor_address = message.actor_address
        actor = self._child_registry.get_actor(actor_address, None)
        actor_status = actor.get('status', None)
        my_addr = self.config.myAddress
        message = ActorStatusResponse(actor_status, sender, my_addr)
        self.send(sender, message)

    def receive(self, message, sender):
        """
        The receive handler to override
        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The sender of the message
        :type sender:  ActorAddress
        """
        pass

    def _loop(self):
        """"
        Run the actor.  Continues to receive until a poisson pill is obtained
        """
        self.running = True
        self._post_start()
        self.fsock.write('DONE WITH POST START\n')
        while self.running:
            message, sender = self.config.mailbox.get()
            addr = self.address.__repr__()
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
        self.__stop_children()

    def _receive(self, message, sender):
        """
        The receieve hidden method helper.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The sender of the message
        :type sender:  ActorAdddress
        """
        fwd = self._forward_as_needed(message, sender)
        if fwd is False:
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
