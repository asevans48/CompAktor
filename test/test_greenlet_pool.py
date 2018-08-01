"""
Test the greenlet pools

@author
"""

import pytest
from atomos.atomic import AtomicInteger

from pools.greenlet_pool import GreenletPool


@pytest.fixture
def pool():
    pool = GreenletPool()
    yield pool
    pool.close()


@pytest.fixture
def atoi():
    atoi = AtomicInteger()
    return atoi


def callback_func(result):
    atoi.get_and_add(1)
    print(result)


def task(str, atoi):
    atoi.get_and_add(1)
    return 'Bye'


def test_submit_task(pool, atoi):
    gt = pool.submit(task, ('Hello', atoi))
    result = gt.get()
    assert(result == 'Bye')
    assert(atoi.get() == 1)


def test_submit_tasks(pool, atoi):
    gts = []
    for i in range(0, 10):
        gt = pool.submit(task, ('hello', atoi))
        gts.append(gt)
    for gt in gts:
        assert(gt.get(timeout=10) == 'Bye')
    assert(atoi.get() == 10)


def test_submit_tasks_with_callback():
    gts = []
    for i in range(0, 10):
        gt = pool.submit(task, ('hello', atoi), call_back=callback_func)
        gts.append(gt)
    for gt in gts:
        assert (gt.get(timeout=10) == 'Bye')
    assert (atoi.get() == 20)
