
import gevent
from gevent.queue import Queue

from actors.addressing import get_address


class BaseActor(gevent.Greenlet):
    """
    The base actor.
    """

    def __init__(self):
        """
        The constructor which initializes the greenlet thread.
        """
        self.inbox = Queue()
        self.myAddress = get_address()
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
