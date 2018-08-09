"""
Test the multiprocessing actor.

@author aevans
"""
import atexit
import traceback

import gevent
import sys
from multiprocessing import Manager, Event, Process

import pytest

from actors.address.addressing import ActorAddress
from actors.base_actor import ActorConfig, WorkPoolType
from actors.multiprocess_actor import MultiprocessBaseActor
from messages.actor_maintenance import StopActor, ActorStopped
from messages.base import BaseMessage
from messages.poison import POISONPILL
from networking.communication import send_message
from networking.socket_server import SocketServerSecurity, create_socket_server
from registry.registry import ActorStatus

ACTORSTARTED = object()
HOST = '127.0.0.1'
PORT = 12000


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

    def __init__(self,
                 actor_config,
                 system_address,
                 host,
                 port,
                 parent,
                 max_threads,
                 signal_queue,
                 message_queue,
                 security=SocketServerSecurity()):
        self.signal_queue = signal_queue
        actor_config.mailbox = message_queue
        Process.__init__(self)
        MultiprocessBaseActor.__init__(actor_config, actor_config.myAddress)
        self.server = create_socket_server(
                host,
                port,
                max_threads,
                signal_queue,
                message_queue,
                security,
                message_handler=None)
        print(self.server)
        self.fsock = None

    def _post_start(self):
        self.fsock.write('STARTED')

    def _post_stop(self):
        self.server.stop_server()
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
        self.fsock = open('/home/aevans/Documents/test/multitest3.txt', 'a')
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

    def __init__(self,
                 actor_config,
                 system_address,
                 host,
                 port,
                 parent,
                 max_threads,
                 signal_queue,
                 message_queue,
                 security=SocketServerSecurity()):
        self.host = host
        self.port = port
        self.__security = security
        actor_config.mailbox = message_queue
        self.signal_queue = signal_queue
        Process.__init__(self)
        MultiprocessBaseActor.__init__(self, actor_config, actor_config.myAddress)
        self.server = None
        self.server_thread = None
        self.fsock = None

    def _post_start(self):
        self.fsock.write('STARTED')

    def _post_stop(self):
        self.server.stop_server()
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

    def setup_server(self):
        self.server = create_socket_server(
            self.host,
            self.port,
            max_threads,
            signal_queue,
            message_queue,
            self.__security,
            message_handler=None)
        self.fsock.write(str(self.server.signal_queue.get(timeout=30)))
        self.fsock.flush()

    def run(self):
        self.fsock = open('/home/aevans/Documents/test/multitest2.txt', 'a')
        sys.stdout = self.fsock
        sys.stderr = self.fsock
        self.fsock.write('STARTING ACTOR \n')
        self.fsock.flush()

        self.fsock.write('STARTING SERVER')
        self.__setup_server()
        self.fsock.write('SERVER STARTED')

        try:
            self.server_thread = gevent.spawn(self.server.run)
            self.server_thread.start()
            self.fsock.write('STARTING SERVER\n')
            self.fsock.flush()
            self.fsock.write(str(self.server.signal_queue.get(timeout=2)))
            self.fsock.flush()
            self.fsock.write('STARTED SERVER\n')
            self.fsock.flush()
            atexit.register(self.server_thread.kill)
            self.server_thread.start()
            self._loop()
        except Exception as e:
            self.fsock.write(traceback.format_exc())
            self.fsock.flush()
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
    assert (test_actor.config.host == '127.0.0.1')
    assert (test_actor.config.port == 12000)


@pytest.mark.order2
def test_actor_start(test_actor):
    test_actor.start()
    msg = test_actor.props['signal_queue'].get(timeout=5)
    print(msg)
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
    sec = test_actor.config.props['sec']
    send_message(msg, None, test_actor.address, sec)
    print(test_actor.config.props['signal_queue'].get(timeout=30))


@pytest.mark.order4
def test_create_actor(test_actor):
    """
    Test the creation of an actor

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    pass

@pytest.mark.order5
def test_forward_to_child(test_actor):
    """
    Test forwarding to an actor

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    pass


@pytest.mark.order6
def test_remove_actor(test_actor):
    """
    Test removing a child actor

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    pass


@pytest.mark.order7
def test_set_actor_status(test_actor):
    """
    Test set the actor status

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    pass


@pytest.mark.order8
def test_broadcast(test_actor):
    """
    Test broadcast to an actor

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    pass


@pytest.mark.order9
def test_stop_actor(test_actor):
    """
    Test stop the actor

    :param test_actor:  The test actor
    :type test_actor:  TestActor
    """
    msg = StopActor(test_actor.address, None)
    print(test_actor.config.__dict__)
    print(test_actor.config.mailbox.empty())
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
    mgr = Manager()
    mgr2 = Manager()
    ev = Event()
    q = mgr.Queue()
    sq = mgr2.Queue()
    config = ActorConfig()
    config.host = '127.0.0.1'
    config.port = 12000
    config.mailbox = mgr.Queue()
    config.work_pool_type = WorkPoolType.GREENLET
    config.props = {'signal_queue': q, 'event': ev}
    parent = []
    max_threads = 1000
    signal_queue = q
    message_queue = config.mailbox
    sec = SocketServerSecurity()
    test_actor = TestActor(config,
                           None,
                           HOST,
                           PORT,
                           parent,
                           max_threads,
                           signal_queue,
                           message_queue,
                           sec)
    test_actor_setup(test_actor)
    test_actor.start()
    print(test_actor.signal_queue.get(timeout=30))
    test_stop_actor(test_actor)
