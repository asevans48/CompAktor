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
from messages.actor_maintenance import SetActorStatus
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

    def __init__(self, actor_config, system_queue):
        """
        Constructor

        :param actor_config:  The actor configuration
        :type actor_config:  ActorConfig
        :param actor_system:  The actor system to use (greenlet based)
        :type actor_system:  ActorSystem
        """
        monkey.patch_all()
        self.config = actor_config
        self.system_queue = system_queue
        self.config.myAddress.host = self.host
        self.config.myAddress.port = self.port
        work_pool_type = self.config.work_pool_type
        max_workers = self.config.max_workers
        self.work_pool = None
        if work_pool_type == WorkPoolType.GREENLET:
            self.work_pool = GreenletPool(
                max_workers=max_workers)
        elif work_pool_type == WorkPoolType.ASNYCIO:
            self.work_pool = AsyncioWorkPool(
                max_workers=max_workers)
        elif work_pool_type == WorkPoolType.PROCESS:
            self.work_pool = MultiProcPool(
                max_workers=max_workers)
        atexit.register(self._cleanup)
        Process.__init__(self)

    def _setup(self):
        """
        Called at actor start to setup the actor
        """
        pass

    def _cleanup(self):
        """
        Called at exit to cause the actor to perform cleanup work.
        """
        try:
            if self.work_pool:
                self.work_pool.close()
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
            self.system_queue.put(message)
        except Exception as e:
            logger = logging.get_logger()
            message = logging.package_error_message()
            logging.log_error(logger, message)

    def send(self, target, message):
        """
        Send the a message to another actor.

        :param target:  The target actor address
        :type target:  ActorAddress
        :param message:  The pickle-able message to send
        :type message:  object
        :return:
        """
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
                self.running=False
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
