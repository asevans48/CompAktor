
import pytest


@pytest.fixture
def system():
    pass


@pytest.fixture
def actor(system):
    pass


def test_message_queue(system, actor):
    pass


def test_message_actor():
    pass


def test_ask_system_pong():
    pass


def test_tell_system():
    pass


def test_actor_stop():
    pass


def test_system_stop():
    pass
