"""
Test the multiprocessing actor.

@author aevans
"""
import multiprocessing
import sys
from multiprocessing import Manager, Queue, Event, Process

import pytest

from actors.address.addressing import ActorAddress, get_address
from actors.base_actor import ActorConfig, WorkPoolType
from actors.multiprocess_actor import MultiprocessBaseActor
from messages.actor_maintenance import StopActor, ActorStopped, CreateActor, RemoveActor, SetActorStatus
from messages.base import BaseMessage
from messages.poison import POISONPILL
from messages.routing import Forward, Broadcast
from registry.registry import ActorStatus

ACTORSTARTED = object()


class ActorChildren(BaseMessage):

    def __init__(self, children, target, sender):
        super(ActorChildren, self).__init__(target, sender)
        self.children = children


class TestPingPongMessage(BaseMessage):

    def __init__(self, target, sender):
        super(TestPingPongMessage, self).__init__(target, sender)


class GetChildren(BaseMessage):

    def __init__(self, target, sender):
        super(GetChildren, self).__init__(target, sender)


class GetChildStatus(BaseMessage):

    def __init__(self, child_address, target, sender):
        super(GetChildStatus, self).__init__(target, sender)
        self.child_address = child_address


class TestActor2(Process, MultiprocessBaseActor):

    def __init__(self, actor_config, system_address, parent=[]):
        Process.__init__(self)
        mgr = Manager()
        actor_config.mailbox = mgr.Queue()
        MultiprocessBaseActor.__init__(self, actor_config, system_address, parent)
        self.mailbox = actor_config.mailbox
        self.signal_queue = actor_config.props['signal_queue']
        self.event = actor_config.props['event']

    def _post_start(self):
        self.signal_queue.put_nowait(ACTORSTARTED)

    def _post_stop(self):
        msg = ActorStopped(ActorStatus.STOPPED, None, self.config.myAddress)
        self.signal_queue.empty()
        self.signal_queue.put_nowait(msg)

    def __handle_get_children(self, message, sender):
        addr = self.config.myAddress
        children = self._child_registry.get_keys()
        msg = ActorChildren(children, sender, addr)
        self.signal_queue.put(msg)

    def __handle_get_child_status(self, message, sender):
        addr = message.child_address
        status = None
        if self._child_registry.get_actor(addr):
            status = self._child_registry.get_actor(addr)['status']
        self.signal_queue.put_nowait(status)

    def create_child(self, message, sender):
        self._handle_create_actor(message, sender)
        return True

    def get_children(self):
        return self._child_registry.get_keys()

    def receive(self, message, sender):
        if type(message) is TestPingPongMessage:
            self.signal_queue.put_nowait(TestPingPongMessage(sender, self.address))
        elif type(message) is GetChildren:
            self.__handle_get_children(message, sender)
        elif type(message) is GetChildStatus:
            self.__handle_get_child_status(message, sender)


    def run(self):
        self.fsock = open('/home/aevans/Documents/test/multitest3.txt', 'w')
        sys.stdout = self.fsock
        sys.stderr = self.fsock
        self.fsock.write('STARTING ACTOR \n')
        self.fsock.flush()
        try:
            self._loop()
        finally:
            self.fsock.flush()
            self.fsock.close()


class TestActor(Process, MultiprocessBaseActor):

    def __init__(self, actor_config, system_address, parent=[]):
        Process.__init__(self)
        mgr = Manager()
        actor_config.mailbox = mgr.Queue()
        MultiprocessBaseActor.__init__(self, actor_config, system_address, parent)
        self.mailbox = actor_config.mailbox
        self.signal_queue = actor_config.props['signal_queue']
        self.event = actor_config.props['event']

    def _post_start(self):
        self.signal_queue.put_nowait(ACTORSTARTED)

    def _post_stop(self):
        msg = ActorStopped(ActorStatus.STOPPED, None, self.config.myAddress)
        self.signal_queue.empty()
        self.signal_queue.put_nowait(msg)

    def __handle_get_children(self, message, sender):
        addr = self.config.myAddress
        children = self._child_registry.get_keys()
        msg = ActorChildren(children, sender, addr)
        self.signal_queue.put(msg)

    def __handle_get_child_status(self, message, sender):
        addr = message.child_address
        status = None
        if self._child_registry.get_actor(addr):
            status = self._child_registry.get_actor(addr)['status']
        self.signal_queue.put_nowait(status)

    def create_child(self, message, sender):
        self._handle_create_actor(message, sender)
        return True

    def get_children(self):
        return self._child_registry.get_keys()

    def receive(self, message, sender):
        if type(message) is TestPingPongMessage:
            self.signal_queue.put_nowait(TestPingPongMessage(sender, self.address))
        elif type(message) is GetChildren:
            self.__handle_get_children(message, sender)
        elif type(message) is GetChildStatus:
            self.__handle_get_child_status(message, sender)


    def run(self):
        self.fsock = open('/home/aevans/Documents/test/multitest2.txt', 'a')
        sys.stdout = self.fsock
        sys.stderr = self.fsock
        self.fsock.write('STARTING ACTOR \n')
        try:
            self._loop()
        finally:
            self.fsock.flush()
            self.fsock.close()


@pytest.fixture
def test_actor():
    mgr = Manager()
    q = mgr.Queue()
    ev = Event()
    config = ActorConfig()
    config.host = ''
    config.port = 12000
    config.mailbox = mgr.Queue()
    config.work_pool_type = WorkPoolType.GREENLET
    config.props = {'signal_queue': q, 'event': ev}
    config.myAddress = ActorAddress('testa', config.host, config.port)
    test_actor = TestActor(config, None)
    yield test_actor
    if test_actor.is_alive():
        test_actor.terminate()
        test_actor.join(timeout=5)


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
    msg = test_actor.props['signal_queue'].get(timeout=5)
    assert(msg is not None)
    assert(type(msg) is POISONPILL)


@pytest.mark.order3
def test_actor_non_default_message(test_actor):
    """
    Test the actor with a non-default message

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    msg = TestPingPongMessage(test_actor.config.myAddress, None)
    addr = test_actor.config.myAddress
    test_actor.config.mailbox.put_nowait((msg, addr))
    msg = test_actor.signal_queue.get(timeout=30)
    assert(type(msg) is TestPingPongMessage)


@pytest.mark.order4
def test_create_actor(test_actor):
    """
    Test the creation of an actor

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    addr = test_actor.config.myAddress
    nconfig = ActorConfig()
    nconfig.host = ''
    nconfig.port = 12000
    nconfig.mailbox = Manager().Queue()
    nconfig.work_pool_type = WorkPoolType.GREENLET
    nconfig.props = {'signal_queue': Manager().Queue(), 'event': Manager().Event()}
    msg = CreateActor(TestActor2, nconfig, [], test_actor.address, None)
    test_actor.mailbox.put((msg, addr))
    #print(sq.get(timeout=10))
    msg = GetChildren(addr, None)
    test_actor.mailbox.put((msg, addr))
    #test_actor.signal_queue.put('Yo')
    msg = test_actor.signal_queue.get(timeout=30)
    assert(len(msg.children)  == 1)


@pytest.mark.order5
def test_forward_to_child(test_actor):
    config = test_actor.config
    addr = test_actor.address
    nconfig = ActorConfig()
    nconfig.host = ''
    nconfig.port = 12000
    nconfig.mailbox = Manager().Queue()
    nconfig.myAddress = get_address(nconfig.host, nconfig.port)
    nconfig.work_pool_type = WorkPoolType.GREENLET
    nconfig.props = {'signal_queue': Manager().Queue(), 'event': Manager().Event()}
    msg = CreateActor(TestActor2, nconfig, [], config.myAddress, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = GetChildren(nconfig.myAddress.address, None)
    chain = [nconfig.myAddress.address, ]
    msg = Forward(msg, chain, nconfig.myAddress, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = nconfig.props['signal_queue'].get(timeout=30)
    msg = nconfig.props['signal_queue'].get(timeout=30)
    assert (len(list(msg.children)) == 0)
    assert (msg.sender.__eq__(nconfig.myAddress))


@pytest.mark.order6
def test_remove_actor(test_actor):
    config = test_actor.config
    addr = test_actor.config.myAddress
    nconfig = ActorConfig()
    nconfig.host = ''
    nconfig.port = 12000
    nconfig.mailbox = Manager().Queue()
    nconfig.myAddress = get_address(nconfig.host, nconfig.port)
    nconfig.work_pool_type = WorkPoolType.GREENLET
    nconfig.props = {'signal_queue': Manager().Queue(), 'event': Manager().Event()}
    msg = CreateActor(TestActor, nconfig, [], config.myAddress, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = GetChildren(addr, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = test_actor.signal_queue.get(timeout=30)
    assert (type(msg) is ActorChildren)
    assert (nconfig.myAddress.address in list(msg.children))
    msg = RemoveActor(nconfig.myAddress, test_actor.address, None)
    test_actor.config.mailbox.put_nowait((msg, None))
    msg = GetChildren(addr, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = test_actor.signal_queue.get(timeout=30)
    assert (nconfig.myAddress.address not in list(msg.children))


@pytest.mark.order7
def test_set_actor_status(test_actor):
    config = test_actor.config
    addr = test_actor.config.myAddress
    nconfig = ActorConfig()
    nconfig.host = ''
    nconfig.port = 12000
    nconfig.mailbox = Manager().Queue()
    nconfig.myAddress = get_address(nconfig.host, nconfig.port)
    nconfig.work_pool_type = WorkPoolType.GREENLET
    nconfig.props = {'signal_queue': Manager().Queue(), 'event': Manager().Event()}
    msg = CreateActor(TestActor, nconfig, [], config.myAddress, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = SetActorStatus(
        nconfig.myAddress, ActorStatus.UNREACHABLE, addr, None)
    test_actor.config.mailbox.put_nowait((msg, addr))
    msg = GetChildStatus(nconfig.myAddress, addr, None)
    test_actor.config.mailbox.put_nowait((msg, addr))
    msg = test_actor.config.props['signal_queue'].get(timeout=30)
    assert(type(msg) is ActorStatus)
    assert(msg == ActorStatus.UNREACHABLE)

@pytest.mark.order8
def test_broadcast(test_actor):
    addr = test_actor.config.myAddress
    nconfig = ActorConfig()
    nconfig.host = ''
    nconfig.port = 12000
    nconfig.myAddress = get_address(nconfig.host, nconfig.port)
    nconfig.mailbox = Manager().Queue()
    nconfig.work_pool_type = WorkPoolType.GREENLET
    nconfig.props = {'signal_queue': Manager().Queue(), 'event': Manager().Event()}
    msg = CreateActor(TestActor2, nconfig, [], test_actor.address, None)
    test_actor.mailbox.put((msg, addr))
    msg = nconfig.props['signal_queue'].get(timeout=30)
    msg = SetActorStatus(
        nconfig.myAddress, ActorStatus.UNREACHABLE, addr, None)
    test_actor.config.mailbox.put_nowait((msg, addr))
    msg = GetChildStatus(nconfig.myAddress, addr, None)
    msg = Broadcast(msg, addr, None)
    test_actor.config.mailbox.put_nowait((msg, addr))
    msg = test_actor.config.props['signal_queue'].get(timeout=30)
    assert (type(msg) is ActorStatus)
    assert (msg == ActorStatus.UNREACHABLE)
    msg = nconfig.props['signal_queue'].get(timeout=30)
    assert(msg is None)


@pytest.mark.order9
def test_stop_actor(test_actor):
    """
    Test stop the actor

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    msg = StopActor(test_actor.address, None)
    test_actor.config.mailbox.put_nowait((msg, test_actor.config.myAddress))
    msg = test_actor.signal_queue.get(timeout=30)
    while test_actor.config.mailbox.empty() is False:
        msg = test_actor.signal_queue.get(timeout=30)
    print(msg)
    assert(msg is not None)
    assert(type(msg) is ActorStopped)
    test_actor.terminate()
    test_actor.join(timeout=10)


if __name__ == "__main__":
    with Manager() as mgr:
        ev = Event()
        q = mgr.Queue()
        sq = mgr.Queue()
        config = ActorConfig()
        config.host = '127.0.0.1'
        config.port = 12000
        config.mailbox = Queue()
        config.work_pool_type = WorkPoolType.GREENLET
        config.props = {'signal_queue': q, 'event': ev}
        print(config.mailbox)
        test_actor = TestActor(config,
                               None)
        test_actor.start()
        test_stop_actor(test_actor)