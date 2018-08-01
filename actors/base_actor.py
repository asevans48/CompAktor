"""
The base actor

@author aevans
"""
import atexit
import socket
import traceback
from enum import Enum
from multiprocessing import Process
from multiprocessing import Queue

import gevent
from gevent import monkey

from logging_handler import logging
from logging_handler.logging import package_error_message
from messages.actor_maintenance import SetActorStatus, RegisterActor
from messages.poison import POISONPILL
from networking.socket_server import SocketServerSecurity
from networking.utils import package_message
from pools.asyncio_work_pool import AsyncioWorkPool
from pools.greenlet_pool import GreenletPool
from pools.multiproc_pool import MultiProcPool
from registry.registry import ActorStatus


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
    host = None
    port = 0
    work_pool_type = WorkPoolType.ASNYCIO
    max_workers = 100
    security_config = SocketServerSecurity()
    mailbox = Queue()
    myAddress = None


class BaseActor(Process):
    """
    The base actor.
    """

    def __init__(self, actor_config, system_address, parent=None):
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
        self.status = ActorStatus.SETUP
        self.config = actor_config
        self.__system_address = system_address
        self.__parent = parent
        self.config.myAddress.host = self.host
        self.config.myAddress.port = self.port
        work_pool_type = self.config.work_pool_type
        max_workers = self.config.max_workers
        self.__work_pool = None
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

    def _get_parent(self):
        return self.__parent

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
            message = SetActorStatus(
                self.config.myAddress,
                ActorStatus.STOPPED)
            self.send(self.__system_address, message)
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message()
            logging.log_error(logger, message)

    def create_actor(self, message, sender):
        try:
            actor_class = message.actor_class
            actor_config = message.actor_config
            parent_address = message.parent_address
            system_address = message.system_address
            if system_address is None:
                raise ValueError('System Not Provided')
            actor = actor_class(actor_config, system_address, parent_address)
            p = actor.start()
            try:
                actor_address = actor.config.myAddress
                register = RegisterActor(actor_address, ActorStatus.RUNNING)
                rval = self.send(system_address, register)
                if rval is False:
                    raise Exception('Failed to Register Actor')
            except Exception as e:
                p.terminate()
                p.join(timeout=10)
                logger = logging.get_logger()
                message = package_error_message()
                logging.log_error(logger, message)
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
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                try:
                    sock.connect((target.host, target.port))
                    sock.send(msg)
                    success = True
                except Exception as e:
                    message = traceback.format_exc()
                    message = package_message(message)
                    logger = logging.get_logger()
                    logging.log_error(logger, message)
                finally:
                    sock.close()
            except Exception as e:
                message = package_error_message()
                logger = logging.get_logger()
                logging.log_error(logger, message)
        return success

    def __stop(self):
        """
        Tell the actor to stop
        """
        self.running = False

    def receive(self, message):
        """
        The receieve method to override.

        :param message:  The message to handle
        :type message:  object
        """
        pass

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
                    self.receive(message.decode())
                except Exception as e:
                    logger = logging.get_logger()
                    message = package_error_message()
                    logging.log_error(logger, message)
                gevent.sleep(0)
        addr_object = SetActorStatus(self.myAddress, ActorStatus.STOPPED)
        self.system_queue.put(addr_object)
