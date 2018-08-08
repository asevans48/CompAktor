import random
from multiprocessing import Process, Queue, Manager
from time import sleep



class OutMe(object):

    def __init__(self, q):
        self.q = q

    def do_put(self):
        self.q.put("Received")

    def rec(self):
        pass


class QMe(OutMe):

    def __init__(self, q, iq):
        self.q = q
        self.iq = iq
        OutMe.__init__(self, q)

    def rec(self):
        msg = self.iq.get(timeout=20)
        return msg

    def run(self):
        pass


class MProc(Process, QMe):

    def __init__(self, queue, oqueue):
        self.queue = queue
        self.oqueue = oqueue.get('oq')
        print(self.queue)
        Process.__init__(self)
        QMe.__init__(self, self.oqueue, self.queue)

    def run(self):
        while True:
            msg = self.rec()
            print(msg)
            self.do_put()


if __name__ == "__main__":
    mgr = Manager()
    mgr2 = Manager()
    mq = mgr.Queue()
    print(mq)
    oq = {'oq': mgr2.Queue()}
    mp = MProc(mq, oq)
    mp.start()
    while True:
        mq.put('Put')
        sleep(random.randint(0,3))
        print('Hello')
        print(oq.get('oq').get())
