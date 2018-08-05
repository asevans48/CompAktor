
import pytest

@pytest.fixture
def test_actor():
    pass

@pytest.mark.order1
def test_actor_setup(test_actor):
    pass

@pytest.mark.order2
def test_actor_start(test_actor):
    pass

@pytest.mark.order3
def test_actor_send(test_actor):
    pass

@pytest.mark.order4
def test_actor_creation(test_actor):
    pass

@pytest.mark.order5
def test_actor_forward(test_actor):
    pass

@pytest.mark.order6
def test_register_global_actor(test_actor):
    pass

@pytest.mark.order7
def test_unregister_global_actor(test_actor):
    pass

@pytest.mark.order8
def test_remove_actor(test_actor):
    pass

@pytest.mark.order9
def test_set_actor_status(test_actor):
    pass

@pytest.mark.order10
def test_stop_actor(test_actor):
    pass
