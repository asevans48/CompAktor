"""
Test the multiprocessing actor.

@author aevans
"""
import multiprocessing
from multiprocessing import Manager, Queue, Event, Process

import pdb
import pytest

from actors.address.addressing import ActorAddress
from actors.base_actor import ActorConfig, WorkPoolType
from actors.multiprocess_actor import MultiprocessActorUtils
from messages.actor_maintenance import StopActor, ActorStopped
from messages.poison import POISONPILL

ACTORSTARTED = object()


class TestActor(Process, MultiprocessActorUtils):

    def __init__(self, actor_config, system_address, parent=[]):
        Process.__init__(self)
        MultiprocessActorUtils.__init__(self, actor_config, system_address, parent)
        self.signal_queue = actor_config.props['signal_queue']
        self.event = actor_config.props['event']

    def _post_start(self):
        self.signal_queue.put_nowait(ACTORSTARTED)
        self.signal_queue.empty()
        self.event.set()

    def _post_stop(self):
        print('PUTTING')
        msg = ActorStopped(None, self.config.myAddress)
        self.signal_queue.empty()
        self.signal_queue.put_nowait(msg)

    def receive(self):
        pass

    def run(self):
        self._loop()


@pytest.fixture
def test_actor():
    q = Queue
    ev = Event()
    config.host = ''
    config.port = 12000
    config.mailbox = Queue()
    config.work_pool_type = WorkPoolType.GREENLET
    config.props = {'signal_queue': q, 'event': ev}
    config.myAddress = ActorAddress('testa', config.host, config.port)
    test_actor = TestActor(config, None)
    return test_actor


@pytest.mark.order1
def test_actor_setup(test_actor):
    """
    Test the actor setup.

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    assert (type(test_actor) is TestActor)
    assert (test_actor.config.host == '')
    assert (test_actor.config.port == 12000)
    assert (type(test_actor.config.mailbox) is Queue)


@pytest.mark.order2
def test_actor_start(test_actor):
    test_actor.start()


@pytest.mark.order3
def test_actor_non_default_message(test_actor):
    pass


@pytest.mark.order4
def test_create_actor(test_actor):
    pass


@pytest.mark.order5
def test_forward_to_child(test_actor):
    pass


@pytest.mark.order6
def test_remove_actor(test_actor):
    pass


@pytest.mark.order7
def test_set_actor_status(test_actor):
    pass


@pytest.mark.order8
def test_broadcast(test_actor):
    pass


@pytest.mark.order9
def test_stop_actor(test_actor):
    """
    Test stop the actor

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    test_actor.config.mailbox.put_nowait((POISONPILL, test_actor.config.myAddress))
    msg = test_actor.signal_queue.get(timeout=30)
    assert (type(msg) is ActorStopped)
    test_actor.kill()


if __name__ == "__main__":
    mgr = Manager()
    ev = Event()
    q = Queue()
    config = ActorConfig()
    config.host = ''
    config.port = 12000
    config.mailbox = Queue()
    config.work_pool_type = WorkPoolType.GREENLET
    config.props = {'signal_queue': q, 'event': ev}
    config.myAddress = ActorAddress('testa', config.host, config.port)
    test_actor = TestActor(config, None)
    test_actor.start()
    ev.wait(timeout=30)
    print("GETTING FROM::: ", q, q.empty())
    msg = q.get(timeout=5)
    print(msg)
    print(type(msg))
    assert (type(test_actor) is TestActor)
    assert (test_actor.config.host == '')
    assert (test_actor.config.port == 12000)
    print(type(test_actor.config.mailbox))
    assert (type(test_actor.config.mailbox) is multiprocessing.queues.Queue)
    test_stop_actor(test_actor)
