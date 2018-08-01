"""
The actor registry.

@author aevans
"""

from gevent.queue import Queue
from registry.registry import ActorRegistry


class ActorSystem(object):

    def __init__(self):
        self.registery = ActorRegistry()
        self.message_queue = Queue()

    def create_actor(self):
        pass

