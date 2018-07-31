
import gevent
from gevent.pool import Pool

from pools.base_pool import BasePool


class GreenletPool(BasePool):

    def __init__(self, max_threads=1000):
        """
        Constructor

        :param max_threads:  The maximum number of threads in the pool
        :type max_threads:  int
        """
        self.pool = Pool(max_threads)

    def submit(self, func, args=None, kwargs=None, call_back=None):
        """
        Submit a function to the greenlet pool

        :param func:  the function to execute
        :type func:  func
        :param args:  Arguments for the application
        :type args:  list
        :param kwargs:  Any kwargs for execution
        :type kwargs:  dict
        :param call_back:  A callback function if applicable
        :type call_back:  func
        :return:  A greenlet thread
        """
        return self.pool.apply_async(func, args, call_back)

    def close_pool(self, timeout=None):
        """
        Close the running pool

        :param timeout:  A timeout to apply to the pool
        :type timeout:  int
        """
        if timeout:
            self.pool.join(timeout)
        else:
            self.pool.join()
