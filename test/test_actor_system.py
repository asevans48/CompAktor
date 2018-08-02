
import pytest

from actors.base_actor import ActorConfig, WorkPoolType, BaseActor
from messages.actor_maintenance import StopActor
from networking.socket_server import SocketServerSecurity
from system.actor_system import ActorSystem, tell, ask


def get_actor_config(global_name):
    conf = ActorConfig()
    conf.security_config = SocketServerSecurity()
    conf.work_pool_type = WorkPoolType.GREENLET
    conf.host = '127.0.0.1'
    conf.port = 12000
    conf.max_workers = 1000
    conf.global_name = global_name
    return conf


@pytest.fixture
def system():
    conf = get_actor_config("system")
    sys = ActorSystem(conf)
    p = sys.start()
    yield (sys, p)
    if p.is_alive:
        p.terminate()
        p.join(timeout=30)


@pytest.fixture
def test_actora(system):
    system_address = system.config.myAddress
    config = get_actor_config("testa")
    actor = BaseActor(config, system_address)
    p = actor.start()
    yield (actor, p)
    message = StopActor(target=actor.config.myAddress, sender=system_address)
    ask(system, message)
    if p.is_alive:
        p.terminate()
        p.join()


@pytest.mark.order1
def test_create_actor():
    pass


@pytest.mark.order2
def test_tell():
    pass


@pytest.mark.order3
def test_ask():
    pass


@pytest.mark.order4
def test_register_actor():
    pass


@pytest.mark.order5
def test_remove_actor():
    pass


@pytest.mark.order6
def test_set_convention_leader():
    pass


@pytest.mark.order7
def test_receive():
    pass


@pytest.mark.order8
def test_global_registration():
    pass


@pytest.mark.order9
def test_stop():
    pass
