
import asyncio
import atexit
import functools

import uvloop

from pools.base_pool import BasePool


POISONPILL = object()


class AsyncioWorkPool(BasePool):

    def __init__(self, event_loop=uvloop.new_event_loop(), max_workers=100):
        """
        Constructor

        :param event_loop:  The event loop to submit to
        :type event_loop:  asyncio.events.AbstractEventLoop
        :param max_workers:  The maximum number of workers for the pool
        :type max_workers:  int
        """
        self.event_loop = event_loop
        asyncio.set_event_loop(event_loop)
        self.queue = asyncio.Queue()
        self.num_workers = max_workers
        self.__workers = []
        for i in range(0, max_workers):
            worker = asyncio.ensure_future(self.worker(), loop=event_loop)
            self.__workers.append(worker)

    async def worker(self):
        """
        The worker used to execute a task.  Sets the futures result.
        """
        while True:
            future, task = await self.queue.get()
            try:
                if task is POISONPILL:
                    future.set_result(True)
                    break
                result = await asyncio.wait_for(task, None, loop=self.event_loop)
                future.set_result(result)
            except Exception as e:
                future.set_result(e)

    def submit(self, coroutine, args=None, kwargs=None, callback=None, callback_kwargs=None):
        """
        Submit a coroutine to the pool
        :param coroutine:  The coroutine to submit
        :type coroutine:  asyncio.coroutine
        :param args:  Any arguments for the coroutine
        :type args:  list
        :param kwargs:  Any dictionary arguments to execute
        :type kwargs:  dict
        :param callback:  Any callback function to execute
        :type callback:  func
        :return:  The future and Task to be executed
        :rtype:  tuple
        """
        future = asyncio.Future(loop=self.event_loop)
        if callback:
            if callback_kwargs:
                future.add_done_callback(functools.partial(callback, kwargs=callback_kwargs))
        task = asyncio.get_event_loop().create_task(coroutine(args, kwargs))
        self.queue.put_nowait((future, task))
        return (future, task)

    def close(self, timeout=None):
        """
        Close the underlying event loop
        :param timeout:  only for compatabiltiy
        :type timeout:  int
        """
        for worker in self.__workers:
            future = self.event_loop.create_future()
            self.queue.put_nowait((future, POISONPILL))
            self.event_loop.run_until_complete(future)
        self.event_loop.close()