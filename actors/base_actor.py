import asyncio
from enum import Enum

import gevent
import uvloop
from gevent.queue import Queue

from actors.addressing import get_address
from pools.asyncio_work_pool import AsyncioWorkPool


class WorkPoolType(Enum):
    ASNYCIO = 1


class BaseActor(gevent.Greenlet):
    """
    The base actor.
    """

    def __init__(self, work_pool_type=WorkPoolType.ASYNCIO):
        """
        The constructor which initializes the greenlet thread.
        """
        self.inbox = Queue()
        self.myAddress = get_address()
        self.work_pool = None
        if work_pool_type == WorkPoolType.ASNYCIO:
            asyncio.set_event_loop(uvloop.new_event_loop())
            self.work_pool = AsyncioWorkPool()
        gevent.Greenlet.__init__(self)

    def receive(self, message):
        """
        The receieve method to override.

        :param message:  The message to handle
        :type message:  object
        """
        pass

    def _run(self):
        """"
        Run the actor
        """
        self.running = True
        while self.running:
            message = self.inbox.get()
            self.receive(message)
            gevent.sleep(0)
