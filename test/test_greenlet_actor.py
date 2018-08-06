from multiprocessing import Queue

import gevent
import pytest

from actors.address.addressing import ActorAddress, get_address
from actors.base_actor import ActorConfig, WorkPoolType
from actors.greenlet_base_actor import GreenletBaseActor
from messages.actor_maintenance import ActorStopped, CreateActor, ActorStarted, RemoveActor
from messages.base import BaseMessage
from messages.poison import POISONPILL
from messages.routing import Forward


class ActorChildren(BaseMessage):

    def __init__(self, children, target, sender):
        super(ActorChildren, self).__init__(target, sender)
        self.children = children


class GetChildren(BaseMessage):

    def __init__(self, target, sender):
        super(GetChildren, self).__init__(target, sender)


class TestPingPongMessage(BaseMessage):

    def __init__(self, target, sender):
        super(TestPingPongMessage, self).__init__(target, sender)


class TestActor(GreenletBaseActor):

    def __init__(self, actor_config, system_address, signal_queue=Queue()):
        super(TestActor, self).__init__(actor_config, system_address)
        self.signal_queue = actor_config.props['signal_queue']

    def _post_stop(self):
        addr = self.config.myAddress
        self.signal_queue.put_nowait(ActorStopped(self.state, None, addr))

    def __handle_get_children(self, message, sender):
        addr = self.config.myAddress
        children = self._child_registry.get_keys()
        msg = ActorChildren(children, sender, addr)
        self.signal_queue.put(msg)

    def receive(self, message, sender):
        if type(message) is TestPingPongMessage:
            self.signal_queue.put_nowait(TestPingPongMessage)
        elif type(message) is GetChildren:
            self.__handle_get_children(message, sender)


class TestActor(GreenletBaseActor):

    def __init__(self, actor_config, system_address, signal_queue=Queue()):
        super(TestActor, self).__init__(actor_config, system_address)
        self.signal_queue = actor_config.props['signal_queue']
        self.signal_queue.put(ActorStarted(None, self.config.myAddress))

    def _post_stop(self):
        addr = self.config.myAddress
        self.signal_queue.put_nowait(ActorStopped(self.state, None, addr))

    def __handle_get_children(self, message, sender):
        addr = self.config.myAddress
        children = self._child_registry.get_keys()
        msg = ActorChildren(children, sender, addr)
        self.signal_queue.put(msg)

    def receive(self, message, sender):
        if type(message) is TestPingPongMessage:
            self.signal_queue.put_nowait(TestPingPongMessage)
        elif type(message) is GetChildren:
            self.__handle_get_children(message, sender)



class TestReceive(GreenletBaseActor):

    def __init__(self, actor_config, system_address, signal_queue=Queue()):
        super(TestReceive, self).__init__(actor_config, system_address)
        self.signal_queue = actor_config.props['signal_queue']

    def _post_Stop(self):
        addr = self.config.myAddress
        self.signal_queue.put_nowait(ActorStopped(self.state, None, addr))

    def __handle_get_children(self, message, sender):
        addr = self.config.myAddress
        children = self._child_registry.get_keys()
        msg = ActorChildren(children, sender, addr)
        self.signal_queue.put(msg)

    def receive(self, message, sender):
        if type(message) is TestPingPongMessage:
            self.signal_queue.put_nowait(message)
        elif type(message) is GetChildren:
            self.__handle_get_children(message, sender)


@pytest.fixture
def test_actor():
    config = ActorConfig()
    config.host = ''
    config.port = 12000
    config.mailbox = gevent.queue.Queue()
    config.work_pool_type = WorkPoolType.GREENLET
    config.myAddress = ActorAddress('testa', config.host, config.port)
    actor = TestActor(config, None)
    return actor


@pytest.fixture
def test_system():
    return None


@pytest.mark.order1
def test_actor_setup(test_actor):
    """
    Test the actor setup.

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    assert(type(test_actor) is TestActor)
    assert(test_actor.config.host == '')
    assert(test_actor.config.port == 12000)
    assert(type(test_actor.config.mailbox) is gevent.queue.Queue)


@pytest.mark.order2
def test_actor_start(test_actor):
    """
    Test start the actor

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    test_actor.start()


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
    nconfig.mailbox = gevent.queue.Queue()
    nconfig.work_pool_type = WorkPoolType.GREENLET
    msg = CreateActor(TestActor, nconfig, [], config.myAddress, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = GetChildren(addr, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = test_actor.signal_queue.get(timeout=30)
    assert(len(msg.children)  == 1)


@pytest.mark.order5
def test_forward_to_child(test_actor):
    nconfig = ActorConfig()
    nconfig.host = ''
    nconfig.port = 12000
    nconfig.mailbox = gevent.queue.Queue()
    nconfig.myAddress = get_address(nconfig.host, nconfig.port)
    nconfig.work_pool_type = WorkPoolType.GREENLET
    nconfig.props = {'signal_queue': gevent.queue.Queue()}
    msg = CreateActor(TestActor, nconfig, [], config.myAddress, None)
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
def test_forward_to_system(test_actor, test_system):
    nconfig = ActorConfig()
    nconfig.host = ''
    nconfig.port = 12000
    nconfig.mailbox = gevent.queue.Queue()
    nconfig.myAddress = get_address(nconfig.host, nconfig.port)
    nconfig.work_pool_type = WorkPoolType.GREENLET
    nconfig.props = {'signal_queue': gevent.queue.Queue()}
    msg = CreateActor(TestActor, nconfig, [], config.myAddress, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = GetChildren(nconfig.myAddress.address, None)
    chain = [nconfig.myAddress.address, ]
    msg = Forward(msg, chain, nconfig.myAddress, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = nconfig.props['signal_queue'].get(timeout=30)
    msg = nconfig.props['signal_queue'].get(timeout=30)
    assert(len(list(msg.children)) == 0)
    assert(msg.sender.__eq__(nconfig.myAddress))


@pytest.mark.order7
def test_remove_actor(test_actor):
    addr = test_actor.config.myAddress
    nconfig = ActorConfig()
    nconfig.host = ''
    nconfig.port = 12000
    nconfig.mailbox = gevent.queue.Queue()
    nconfig.work_pool_type = WorkPoolType.GREENLET
    nconfig.props = {'signal_queue': gevent.queue.Queue()}
    msg = CreateActor(TestActor, nconfig, [], config.myAddress, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = GetChildren(addr, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = test_actor.signal_queue.get(timeout=30)
    assert(type(msg) is ActorChildren)
    assert (nconfig.myAddress.address in list(msg.children))
    msg = RemoveActor(nconfig.myAddress, test_actor.address, None)
    test_actor.config.mailbox.put_nowait((msg, None))
    msg = GetChildren(addr, None)
    test_actor.config.mailbox.put((msg, addr))
    msg = test_actor.signal_queue.get(timeout=30)
    assert(nconfig.myAddress.address not in list(msg.children))

@pytest.mark.order8
def test_set_actor_status(test_actor):
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
    assert(type(msg) is ActorStopped)
    test_actor.kill()


if __name__ == "__main__":
    config = ActorConfig()
    config.host = ''
    config.port = 12000
    config.mailbox = gevent.queue.Queue()
    config.work_pool_type = WorkPoolType.GREENLET
    config.props = {'signal_queue': gevent.queue.Queue()}
    test_actor = TestActor(config, None)
    test_actor.start()
    config.props['signal_queue'].get(timeout=30)
    test_remove_actor(test_actor)
    test_stop_actor(test_actor)
