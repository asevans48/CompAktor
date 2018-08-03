from multiprocessing import Queue, Process
from time import sleep

import gevent
from gevent.queue import Queue as GQueue

class Proc(Process):

    def __init__(self, q):
        self.q = q
        Process.__init__(self)

    def run(self):
        while True:
            self.q.put('Hello')
            sleep(1)


class PingPong(gevent.Greenlet):

    def __init__(self, q, gq):
        self.q = q
        self.gq = gq
        Process.__init__(self)

    def start(self):
        while True:
            msg = self.q.get()
            print(msg)
            self.gq.put(msg)
            gevent.sleep(0)


if __name__ == "__main__":
    q = Queue(maxsize=100)
    gq = GQueue(maxsize=100)
    p1 = Proc(q)
    p2 = Proc(q)
    p1 = p1.start()
    p2 = p2.start()
    pp = PingPong(q, gq)
    gevent.spawn(pp.start)
    pp.start()
    while True:
        print("Getting")
        print(gq.get())
