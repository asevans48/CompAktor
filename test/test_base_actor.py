
import re

import pytest

from actors.address.addressing import ActorAddress
from actors.base_actor import BaseActor, ActorConfig, WorkPoolType
from networking.socket_server import SocketServerSecurity


@pytest.fixture
def test_actor1():
    config = ActorConfig()
    config.host = '127.0.0.1'
    config.port = 12000
    config.security_config = SocketServerSecurity()
    config.work_pool_type = WorkPoolType.GREENLET
    address = ActorAddress('testa', config.host, config.port)
    actor = BaseActor(actor_config=config, system_address=address)
    return actor

def test_actor_setup(test_actor1):
    assert (re.match('[0-9\.]+\_\d+\_\d+', test_actor1.address.address) is not None)
    assert (type(test_actor1._parent) is list)
    assert (type(test_actor1._global_actors) is dict)
    assert (test_actor1.config.host == '127.0.0.1')
    assert (test_actor1.config.port == 12000)
