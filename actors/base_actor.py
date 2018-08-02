"""
The base actor

@author aevans
"""
import atexit
from copy import deepcopy
from enum import Enum
from multiprocessing import Process
from multiprocessing import Queue

import gevent
from gevent import monkey

from logging_handler import logging
from logging_handler.logging import package_error_message
from messages.actor_maintenance import SetActorStatus, StopActor
from messages.base import BaseMessage
from messages.poison import POISONPILL
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


class BaseActor(Process):
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
        self.config = actor_config
        self.__system_address = system_address
        self._parent = parent
        self.config.myAddress.host = self.host
        self.config.myAddress.port = self.port
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
            message = logging.package_error_message()
            logging.log_error(logger, message)
        try:
            self.config.mailbox.close()
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message()
            logging.log_error(logger, message)
        try:
            child_keys = self.__child_registry.keys()
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
                        message = package_error_message()
                        logging.log_error(logger, message)
                    finally:
                        p = child['actor_proc']
                        if p:
                            p.terminate()
                            p.join(timeout=15)
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message()
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
            message = logging.package_error_message()
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
            parent_address = message.parent_address
            if self.__system_address is None:
                raise ValueError('System Not Provided')
            actor = actor_class(
                actor_config, self.__system_address, parent_address)
            p = actor.start()
            actor_parent = deepcopy(self._parent)
            actor_parent.append(self.config.myAddress)
            self.__child_registry.add_actor(
                actor.config.myAddress,
                ActorStatus.RUNNING,
                actor.config.mailbox,
                actor_proc=p,
                parent=actor_parent)
        except Exception as e:
            logger = logging.get_logger()
            message = package_error_message()
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
        msg = package_message(
            message,
            self.myAddress,
            self.server_security,
            target)
        if target and target.host and target.port:
            try:
                sender = self.config.myAddress
                send_message(msg, sender, target, self.config.security_config)
            except Exception as e:
                message = logging.package_error_message()
                logger = logging.get_logger()
                logging.log_error(logger, message)
        return success

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

    def __handle_broadcast(self, message, sender):
        """
        The message to broadcast

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
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

    def __handle_ask(self, message, sender):
        """
        The message to ask

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        msg = message.message
        rval = self.receive(msg)
        if rval and issubclass(rval, BaseMessage):
            omessage = Ask(rval)
        else:
            my_addr = self.config.myAddress
            omessage = ReturnMessage(rval, sender, my_addr)
        self.send(sender, omessage)

    def __handle_tell(self, message, sender):
        """
        The message to tell

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        self.receive(message.message, sender)

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


    def __receive(self, message, sender):
        """
        The receieve hidden method helper.

        :param message:  The message to handle
        :type message:  BaseMessage
        :param sender:  The sender of the message
        :type sender:  ActorAdddress
        """
        try:
            if type(message) is Broadcast:
                self.__handle_broadcast(message, sender)
            elif type(message) is Tell:
                self.__handle_tell(message, sender)
            elif type(message) is Ask:
                return self.__handle_ask(message, sender)
            elif type(message) is Forward:
                return self.__handle_forward(message, sender)
            else:
                return self.receive(message, sender)
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message()
            logging.log_error(logger, message)

    def __unpack_message(self, message):
        """
        Unpack the received message.

        :param message:  The received message
        :type message:  dict
        :return: A tuple of the message and sender address
        :rtype:  tuple
        """
        msg = message.get('message', None)
        sender = message.get('sender', None)
        return (msg, sender)

    def start(self):
        """"
        Run the actor.  Continues to receive until a poisson pill is obtained
        """
        self.running = True
        while self.running:
            message = self.inbox.get()
            if type(message) is POISONPILL:
                self.running = False
            else:
                try:
                    message = message.decode()
                    message, sender = self.__unpack_message(message)
                    self.__receive(message, sender)
                except Exception as e:
                    logger = logging.get_logger()
                    message = package_error_message()
                    logging.log_error(logger, message)
                gevent.sleep(0)
        addr_object = SetActorStatus(self.myAddress, ActorStatus.STOPPED)
        self.system_queue.put(addr_object)
