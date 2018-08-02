
import pytest

from actors.base_actor import BaseActor, ActorConfig, WorkPoolType
from networking.socket_server import SocketServerSecurity





@pytest.fixture
def base_actor():
    config = ActorConfig()
    config.host = '127.0.0.1'
    config.port = 0
    config.security_config = SocketServerSecurity()
    config.work_pool_type = WorkPoolType.GREENLET
    actor = BaseActor()
    yield actor
    assert(actor.running == False)


@pytest.mark.order1
def test_child_placement():
    pass


@pytest.mark.order2
def test_place_on_actor_queue():
    pass


@pytest.mark.order3
def test_create_actor():
    pass


@pytest.mark.order4
def test_poison_pill():
    pass
