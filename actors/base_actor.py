"""
The base actor.

Every actor
    - maintains a child registry
    - may or may not have a work pool
    - can be stopped
    - can create child actors
    - sends and receives messages
    - has an independent state
    - runs independent of other actors

The first five components are implemented in BaseActor

@author aevans
"""
import atexit
from copy import deepcopy
from enum import Enum
from multiprocessing import Process
from multiprocessing import Queue

from gevent import monkey

from actors.address.addressing import get_address, ActorAddress
from logging_handler import logging
from logging_handler.logging import package_error_message
from messages.actor_maintenance import SetActorStatus, StopActor, CreateActor, UnRegisterGlobalActor, \
    RegisterGlobalActor, RemoveActor, GetActorStatus, ActorStatusResponse
from messages.base import BaseMessage
from messages.routing import Forward, Broadcast, Tell, Ask, ReturnMessage
from networking.communication import send_message
from networking.socket_server import SocketServerSecurity
from networking.utils import package_message
from pools.asyncio_work_pool import AsyncioWorkPool
from pools.greenlet_pool import GreenletPool
from pools.multiproc_pool import MultiProcPool
from registry.registry import ActorStatus, ActorRegistry


class WorkPoolType(Enum):
    """
    Work pool types
    """
    ASNYCIO = 1
    GREENLET = 2
    PROCESS = 3
    NO_POOL = 4


class ActorConfig(object):
    """
    Work Pool Config
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_config = None
    apm_config = None
    global_name = None
    host = '127.0.0.1'
    port = 12000
    work_pool_type = WorkPoolType.ASNYCIO
    max_workers = 100
    security_config = SocketServerSecurity()
    mailbox = Queue()
    myAddress = None
    convention_leader = None


class BaseActor(object):
    """
    The base actor.
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
        monkey.patch_all()
        self._global_actors = {}
        self.config = actor_config
        self.__system_address = system_address
        self._parent = parent
        self.address = get_address(system_address.host, system_address.port)
        self.config.myAddress = self.address
        print(self.config)
        work_pool_type = self.config.work_pool_type
        max_workers = self.config.max_workers
        self.__work_pool = None
        self.__child_registry = ActorRegistry()
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
        return self.__system_address

    def _get_parent(self):
        """
        Get the parent address.

        :return:  The parent address
        :rtype:  ActorAddress
        """
        return self._parent

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
            self.config.mailbox.close()
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message(self.address)
            logging.log_error(logger, message)
        try:
            child_keys = self.__child_registry.get_keys()
            if len(child_keys):
                for key in child_keys:
                    child = self.__child_registry[key]
                    try:
                        message = StopActor(child['address'], self.config.myAddress)
                        addr_chain = deepcopy(self._parent)
                        addr_chain.append(child['address'])
                        wrapper = Forward(
                            message, addr_chain, child['address'], self.config.myAddress)
                        child.mailbox.put(wrapper, timeout=30)
                    except Exception as e:
                        logger = logging.get_logger()
                        message = package_error_message(self.address)
                        logging.log_error(logger, message)
                    finally:
                        p = child['actor_proc']
                        if p:
                            p.terminate()
                            p.join(timeout=15)
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message(self.address)
            logging.log_error(logger, message)
        try:
            message = SetActorStatus(
                self.config.myAddress,
                ActorStatus.STOPPED,
                self.__system_address,
                self.config.myAddress)
            self.send(self.__system_address, message)
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message(self.address)
            logging.log_error(logger, message)

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
            if self.__system_address is None:
                raise ValueError('System Not Provided')
            actor = actor_class(
                actor_config, self.__system_address, actor_parent)
            p = actor.start()
            self.__child_registry.add_actor(
                actor.config.myAddress,
                ActorStatus.RUNNING,
                actor.config.mailbox,
                actor_proc=p,
                parent=actor_parent)
        except Exception as e:
            logger = logging.get_logger()
            message = package_error_message(self.address)
            logging.log_error(logger, message)

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
                self.myAddress,
                self.server_security,
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

    def __stop(self):
        """
        Tell the actor to stop
        """
        self.running = False

    def receive(self, message, sender):
        """
        The receive handler to override
        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The sender of the message
        :type sender:  ActorAddress
        """
        pass

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
            if self.__child_registry.get_actor(next):
                child = self.__child_registry[next]
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
        target = message.target
        host = self.config.myAddress.host
        port = self.config.myAddress.port
        if target.host != host or target.port != port:
            self.send(target, message)
        if target and message.target.address is not self.config.myAddress.address:
            self._forward_message(message, sender)
            return True
        return False

    def __handle_broadcast(self, message, sender):
        """
        The message to broadcast

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        if self._forward_as_needed() is False:
            message_dict = {'message': message, 'sender': sender}
            for child in self.__child_registry.keys():
                try:
                    inf = self.__child_registry[child]
                    mailbox = inf['mailbox']
                    mailbox.put_no_wait(message_dict)
                except Exception as e:
                    logger = logging.get_logger()
                    message = "Failed to Broadcast {} --> {}".format(
                        self.config.myAddress,
                        child['address']
                    )
                    logging.log_error(logger, message)
        elif self._global_actors.get(message.target.address, None):
            self._forward_message(message, sender)

    def __handle_ask(self, message, sender):
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

    def __handle_tell(self, message, sender):
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

    def __handle_forward(self, message, sender):
        msg = message.message
        chain = message.address_chain
        if chain and len(chain) > 0:
            addr = chain.pop(0)
            if self.__child_registry.has_actor(addr):
                target_addr = self.__child_registry.get_actor(addr)
                target_addr = target_addr['address']
                new_fwd = Forward(msg, chain, target_addr, sender)
                self.send(target_addr, new_fwd)
        else:
            self.receive(msg, message.sender)

    def __handle_create_actor(self, message, sender):
        """
        Handle a request to create an actor.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        actor_class = message.actor_class
        actor_config = message.actor_config
        parent_addr = [self.config.myAddress.address, ]
        actor = actor_class(actor_config, self.config.myAddress, parent_addr)
        actor_p = actor.start()
        self.__child_registry.add_actor(
            actor.config.myAddress,
            ActorStatus.RUNNING,
            actor.config.mailbox,
            actor_p,
            parent_addr)

    def __handle_register_global_actor(self, message, sender):
        """
        Handle a request to register a global actor.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        actor_address = message.actor_address.address
        self._global_actors[actor_address] = message.actor_address

    def __handle_unregister_global_actor(self, message, sender):
        """
        Handle a request to unregister a global actor.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        actor_address = message.actor_address.address
        self._global_actors.pop(actor_address)

    def __handle_remove_actor(self, message, sender):
        """
        Handle a request to remove an actor.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        actor_address = message.actor_address.address
        if self.__child_registry.get(actor_address, None):
            self.__child_registry.remove_actor(actor_address)

    def __handle_set_actor_status(self, message, sender):
        """
        Handle a request to set a child actor status

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        actor_addr = message.actor_address
        actor_status = message.status
        self.__child_registry.set_actor_status(actor_addr, actor_status)

    def __handle_stop_actor(self, message, sender):
        """
        Handle a request to stop the entire system.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        self.__stop()

    def __handle_get_actor_status(self, message, sender):
        """
        Handle a request to get an actor status.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        actor_address = message.actor_address
        actor = self.__child_registry.get_actor(actor_address, None)
        actor_status = actor.get('status', None)
        my_addr = self.config.myAddress
        message = ActorStatusResponse(actor_status, sender, my_addr)
        self.send(sender, message)

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
                    self.__handle_broadcast(message, sender)
                elif type(message) is Tell:
                    self.__handle_tell(message, sender)
                elif type(message) is Ask:
                    return self.__handle_ask(message, sender)
                elif type(message) is Forward:
                    return self.__handle_forward(message, sender)
                elif type(message) is CreateActor:
                    self.__handle_create_actor(message, sender)
                elif type(message) is UnRegisterGlobalActor:
                    self.__handle_unregister_global_actor(message, sender)
                elif type(message) is RegisterGlobalActor:
                    self.__handle_register_global_actor(message, sender)
                elif type(message) is RemoveActor:
                    self.__handle_remove_actor(message, sender)
                elif type(message) is SetActorStatus:
                    self.__handle_set_actor_status(message, sender)
                elif type(message) is StopActor:
                    self.__handle_stop_actor(message, sender)
                elif type(message) is GetActorStatus:
                    self.__handle_get_actor_status(message, sender)
                else:
                    return self.receive(message, sender)
            except Exception as e:
                logger = logging.get_logger()
                message = logging.package_error_message(self.address)
                logging.log_error(logger, message)
        elif self._global_actors.get(message.target.address, None):
            self._forward_message(message, sender)
