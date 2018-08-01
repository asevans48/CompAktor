
class BasePool(object):

    def submit(self, func, args, kwargs, callback=None):
        """
       Submit a task to the pool

        :param func:  The function to execute
        :type func:  func
        :param args:  Any arguments to submit
        :type args:  list
        :param kwargs:  Any kwargs
        :type kwargs:  dict
        :param callback:  Any callback to execute
        :type callback:  func
        :return: object
        """
        raise NotImplemented('Submit Not Yet Implemented in Pool')

    def close(self, timeout=None):
        """
        Close the pool
        :param timeout:  The time to wait if applicable
        :type timeout:  int
        """
        raise NotImplemented('Close Not Yet Implemented')
