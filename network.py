import argparse
import codecs
import socket
import threading
from time import sleep
import random
import rdt


## command-line options for the simulated error rates, shared by client and server
def add_error_args(parser):
    parser.add_argument('--loss', type=float, default=None, help='Packet loss probability (default: 0.1).')
    parser.add_argument('--corrupt', type=float, default=None, help='Packet corruption probability (default: 0.1).')
    parser.add_argument('--reorder', type=float, default=None, help='Packet reorder probability (default: 0.1).')


## Provides an abstraction for the network layer
class NetworkLayer:
    # configuration parameters
    prob_pkt_loss = 0.1
    prob_byte_corr = 0.1
    prob_pkt_reorder = 0.1

    sock = None
    conn = None
    collect_thread = None
    stop = None
    closed = False  # the peer closed the connection
    socket_timeout = 0.1
    reorder_msg_S = None

    def __init__(self, role_S, server_S, port, loss=None, corrupt=None, reorder=None):
        if loss is not None: self.prob_pkt_loss = loss
        if corrupt is not None: self.prob_byte_corr = corrupt
        if reorder is not None: self.prob_pkt_reorder = reorder
        # data read by the collector thread and not delivered yet
        self.buffer_S = ''
        self.lock = threading.Lock()

        print(f'Network: loss={self.prob_pkt_loss} corruption={self.prob_byte_corr} reorder={self.prob_pkt_reorder}')

        if role_S == 'client':
            print('Network: role is client')
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conn.connect((server_S, port))
            self.conn.settimeout(self.socket_timeout)

        elif role_S == 'server':
            print('Network: role is server')
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind(('', port))
            self.sock.listen(1)
            self.conn, addr = self.sock.accept()
            self.conn.settimeout(self.socket_timeout)

        # start the thread to receive data on the connection
        self.collect_thread = threading.Thread(name='Collector', target=self.collect)
        self.stop = False
        self.collect_thread.start()

    def disconnect(self):
        if self.collect_thread:
            self.stop = True
            self.collect_thread.join()
        if self.sock is not None: self.sock.close()
        if self.conn is not None: self.conn.close()

    def udt_send(self, msg_S):
        # return without sending if the packet is being dropped
        if random.random() < self.prob_pkt_loss:
            return
        
        # corrupt a packet: replace 1 to 5 consecutive characters with 'X'
        if random.random() < self.prob_byte_corr:
            start = random.randint(rdt.Packet.size_len, len(msg_S) - 5)
            num = random.randint(1, 5)
            repl_S = ''.join(random.sample('XXXXX', num))
            msg_S = msg_S[:start] + repl_S + msg_S[start + num:]
            
        # reorder packets - either hold a packet back, 
        # or if there is one already held back, then, send both
        if random.random() < self.prob_pkt_reorder or self.reorder_msg_S:
            if self.reorder_msg_S is None:
                self.reorder_msg_S = msg_S
                return None
            else:
                msg_S += self.reorder_msg_S
                self.reorder_msg_S = None

        # keep calling send until all the bytes are transferred
        msg_bytes = msg_S.encode('utf-8')
        totalsent = 0
        try:
            while totalsent < len(msg_bytes):
                sent = self.conn.send(msg_bytes[totalsent:])
                if sent == 0:
                    raise RuntimeError("socket connection broken")
                totalsent = totalsent + sent
        except ConnectionError:
            # the peer is gone, so the packet is lost; the collector thread
            # sees the same error and sets `closed`
            pass

    ## Receive data from the network and save in internal buffer
    def collect(self):
        # keeps the start of a character whose bytes were split across two reads
        decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')

        while (True):
            try:
                recv_bytes = self.conn.recv(2048)
                if not recv_bytes:
                    self.closed = True
                    return
                with self.lock:
                    self.buffer_S += decoder.decode(recv_bytes)
            except BlockingIOError as err:
                pass
            except socket.timeout as err:
                pass
            except ConnectionError:
                self.closed = True
                return
            if self.stop:
                return

    ## Deliver collected data to client
    def udt_receive(self):
        with self.lock:
            ret_S = self.buffer_S
            self.buffer_S = ''
        return ret_S


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Network layer implementation.')
    parser.add_argument('role', help='Role is either client or server.', choices=['client', 'server'])
    parser.add_argument('server', help='Server.')
    parser.add_argument('port', help='Port.', type=int)
    args = parser.parse_args()

    network = NetworkLayer(args.role, args.server, args.port)
    if args.role == 'client':
        network.udt_send('MSG_FROM_CLIENT')
        sleep(2)
        print(network.udt_receive())
        network.disconnect()

    else:
        sleep(1)
        print(network.udt_receive())
        network.udt_send('MSG_FROM_SERVER')
        network.disconnect()
