"""
Test the asyncio pool

@author aevans
"""
from asyncio import coroutine
from atomos.atomic import AtomicInteger

from pools.asyncio_work_pool import AsyncioWorkPool
import pytest


@pytest.fixture
def pool():
    pool = AsyncioWorkPool()
    yield pool
    pool.close()


@coroutine
def simple_test_coroutine(args, kwargs):
    return 'Hello'


def callback_func(future, kwargs=None):
    kwargs['atoi'].get_and_add(1)


def test_submit_task(pool):
    future, task = pool.submit(simple_test_coroutine)
    pool.event_loop.run_until_complete(future)
    assert(future.result() == 'Hello')


def test_multiple_tasks(pool):
    pool = AsyncioWorkPool()
    futs = []
    for i in range(0, 10):
        future, task = pool.submit(simple_test_coroutine)
        futs.append(future)
    for future in futs:
        pool.event_loop.run_until_complete(future)
        assert(future.result() == 'Hello')


def test_callback(pool):
    futs = []
    atomint = AtomicInteger()
    for i in range(0, 10):
        future, task = pl.submit(simple_test_coroutine, callback=callback_func, callback_kwargs={'atoi': atomint})
        futs.append(future)
    for future in futs:
        pl.event_loop.run_until_complete(future)
        assert(future.result() == 'Hello')
    assert(atomint.get() == 10)
