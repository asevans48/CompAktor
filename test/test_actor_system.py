
import pytest

from actors.base_actor import ActorConfig, WorkPoolType, BaseActor
from networking.socket_server import SocketServerSecurity
from system.actor_system import ActorSystem


def get_actor_config(global_name):
    conf = ActorConfig()
    conf.security_config = SocketServerSecurity()
    conf.work_pool_type = WorkPoolType.GREENLET
    conf.host = '127.0.0.1'
    conf.port = 12000
    conf.max_workers = 1000
    conf.global_name = global_name
    return conf


def get_test_actor(system_address):
    config = get_actor_config("testa")
    actor = BaseActor(config, system_address)
    p = actor.start()
    return (actor, p)

@pytest.fixture
def system():
    conf = get_actor_config("system")
    sys = ActorSystem(conf)
    p = sys.start()
    yield (sys, p)
    if p.is_alive:
        p.terminate()
        p.join(timeout=30)


def test_tell():
    pass


def test_ask():
    pass


def test_register_actor():
    pass


def test_remove_actor():
    pass


def test_set_convention_leader():
    pass


def test_receive():
    pass


def test_stop():
    pass
