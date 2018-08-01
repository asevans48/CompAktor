"""
Test the multiprocess pool

@author aevans
"""
from multiprocessing import Process

import pytest
from atomos.atomic import AtomicInteger

from pools.multiproc_pool import MultiProcPool


@pytest.fixture
def pool():
    pool = MultiProcPool()
    yield pool
    pool.close()


@pytest.fixture
def atoi():
    atoi = AtomicInteger()
    return atoi


def callback_func(result):
    print(result)
    return result


def task():
    return 1


def test_submit_task(pool):
    p = pool.submit(task)
    assert(p.get() == 1)


def test_submit_tasks(pool):
    procs = []
    for i in range(0, 10):
        p = pool.submit(task)
        procs.append(p)
    procs = [p.get() for p in procs]
    assert(sum(procs) == 10)


def test_submit_tasks_with_callback(pool):
    procs = []
    for i in range(0, 10):
        p = pool.submit(task, callback=callback_func)
        procs.append(p)
    procs = [p.get() for p in procs]
    assert(sum(procs) == 10)
