
import pytest

from actors.base_actor import BaseActor, ActorConfig, WorkPoolType
from messages.actor_maintenance import StopActor
from networking.communication import send_message
from networking.socket_server import SocketServerSecurity
from system.actor_system import ActorSystem

class ActorSystemWrapper():

    def __init__(self, system, proc):
        self.system = system
        self.proc = proc


@pytest.fixture
def actor_config():
    config = ActorConfig()
    config.host = '127.0.0.1'
    config.port = 0
    config.security_config = SocketServerSecurity()
    config.work_pool_type = WorkPoolType.GREENLET
    return config


@pytest.fixture
def actor_system(actor_config):
    system = ActorSystem(
        actor_config,
        host='127.0.0.1',
        port=12000
    )
    system_proc = system.start()
    sys_wrapper = ActorSystemWrapper(system, system_proc)
    yield sys_wrapper
    sec_conf = actor_config.security_config
    send_message(
        StopActor,
        sys_wrapper.system.config.myAddress,
        sys_wrapper.system.config.myAddress,
        sys_wrapper.config.security_config)
    system_proc.join()



@pytest.fixture
def test_actor1(actor_config):
    actor = BaseActor(actor_config=actor_config)
    yield actor
    assert (actor.running == False)


@pytest.mark.order1
def test_actor_setup(actor_config, actor_system, test_actor1):
    pass


if __name__ == "__main__":
    config = ActorConfig()
    config.host = '127.0.0.1'
    config.port = 0
    config.security_config = SocketServerSecurity()
    config.work_pool_type = WorkPoolType.GREENLET
    system = ActorSystem(
        config,
        host='127.0.0.1',
        port=12000
    )
    actor = BaseActor(actor_config=config, system_address=system.config.myAddress)
    print(actor.get_system_address())
    print(actor.config.myAddress)
