import datetime
import multiprocessing
import socket
from time import sleep


class ConnectionPool():

    def __init__(self, num_socks=10000):
        self.num_socks = num_socks
        self.socks = []

    def start_sock(self):
        pass

    def release_sockets(self):
        pass

    def close_socks(self):
        pass

def run_conn(i):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect(('127.0.0.1', 8888))
        sock.send('Hello Dick {}'.format(i).encode('utf-8'))
        sock.recv(1024)
    except Exception as e:
        return False
    finally:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except Exception as e:
            pass
        sock.close()
    return True

if __name__ == '__main__':
    try:
        trials = []
        for i in range(0,32):
            pool = multiprocessing.Pool()
            try:
                print('Test {}\n'.format(i))
                time = datetime.datetime.now()
                list = [x for x in range(0,1000)]
                print(pool.map(run_conn, list))
                s = (datetime.datetime.now() - time)
                print(s)
                trials.append(s)
            finally:
                pool.close()
        print(sum(trials, datetime.timedelta(0))/ len(trials))
    finally:
        print('Complete')

