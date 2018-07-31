
from pools.base_pool import BasePool

from multiprocessing import Pool


class MultiProcPool(BasePool):

    def __init__(self, max_workers):
        """
        Constructor

        :param max_workers:  Maximum number of workers
        """
        self.max_workers = max_workers
        self.pool = Pool(max_workers)

    def submit(self, func, args=None, kwargs=None, callback=None):
        """
        Submit a task to the pool

        :param func:  The function to execute
        :type func:  func
        :param args:  The arguments for the function
        :type args:   list
        :param kwargs:  Any kwargs to execute
        :type kwargs:  dict
        :param callback:  The function to execute when the process finishes
        :type callback:  func
        """
        self.pool.apply_async(func, args, kwargs, callback)

    def close(self, timeout=None):
        """
        Close the running pool

        :param timeout:  The timeout to wait for the pool to finish
        :type timeout:  int
        """
        self.pool.terminate()
        self.pool.join()
        self.pool.close()
