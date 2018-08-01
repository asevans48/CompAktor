"""
The base actor

@author aevans
"""
from enum import Enum

import gevent
from gevent.queue import Queue

from actors.address.addressing import get_address
from messages.poison import POISONPILL
from networking.utils import send_message_to_actor
from pools.asyncio_work_pool import AsyncioWorkPool
from pools.greenlet_pool import GreenletPool
from pools.multiproc_pool import MultiProcPool


class WorkPoolType(Enum):
    ASNYCIO = 1
    GREENLET = 2
    PROCESS = 3
    NO_POOL = 4


class ActorConfig(object):
    global_name=None
    host=None
    port=0
    work_pool_type=WorkPoolType.ASNYCIO
    max_workers=100
    mailbox=Queue()


class BaseActor(gevent.Greenlet):
    """
    The base actor.
    """

    def __init__(self, actor_config):
        """
        The constructor which initializes the greenlet thread.
        """
        self.inbox = actor_config.mailbox
        self.host = actor_config.host
        self.port = actor_config.port
        self.myAddress = get_address()
        self.myAddress.host = self.host
        self.myAddress.port = self.port
        work_pool_type = actor_config.work_pool_type
        max_workers = actor_config.max_workers
        self.work_pool = None
        if work_pool_type == WorkPoolType.ASNYCIO:
            self.work_pool = AsyncioWorkPool(max_workers=max_workers)
        elif work_pool_type == WorkPoolType.GREENLET:
            self.work_pool = GreenletPool(max_workers=max_workers)
        elif work_pool_type == WorkPoolType.PROCESS:
            self.work_pool = MultiProcPool(max_workers=max_workers)
        gevent.Greenlet.__init__(self)

    def send(self, target, message):
        """
        Send the
        :param target:  The target actor address
        :type target:  ActorAddress
        :param message:  The pickle-able message to send
        :type message:  object
        :return:
        """
        pass

    def receive(self, message):
        """
        The receieve method to override.

        :param message:  The message to handle
        :type message:  object
        """
        pass

    def _run(self):
        """"
        Run the actor.  Continues to receive until a poisson pill is obtained
        """
        self.running = True
        while self.running:
            message = self.inbox.get()
            if type(message) is POISONPILL:
                self.running=False
            else:
                self.receive(message.decode())
                gevent.sleep(0)
