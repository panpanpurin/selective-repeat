import network
import collections
import threading
import time
import hashlib

debug = True


def debug_log(message):
    if debug:
        print(message)

# format a time.time_ns() value in local time with microseconds
def format_time(val):
    f_time = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(val // 1000000000))
    us = (val // 1000) % 1000000
    return f_time + f".{us:06d}"

class Packet:
    # packet = [size, seq_num, ackFlag, checksum, data]

    # values of the ackFlag field
    DATA = 0
    ACK = 1
    FIN = 2  # end of message, sent and ACKed like a data packet

    # field lengths in chars
    size_len = 8
    seq_num_len = 8
    checksum_len = 32  # md5 in hex

    size = None
    size_str = None
    seq_num = None
    seq_num_str = None
    ack = None
    ack_str = None
    checksum = None
    msg = None
    packet = None

    def __init__(self, size_len=8, seq_num_len=8, checksum_len=32):
        self.size_len = size_len
        self.seq_num_len = seq_num_len
        self.checksum_len = checksum_len

    def code(self, seq_num, msg, is_ack=0):

        self.size = self.size_len + self.seq_num_len + self.checksum_len + 1 + len(msg)
        self.size_str = str(self.size).zfill(self.size_len)

        self.seq_num = seq_num
        self.seq_num_str = str(seq_num).zfill(self.seq_num_len)

        self.ack = is_ack
        self.ack_str = str(self.ack)

        self.checksum = hashlib.md5((self.size_str + self.seq_num_str + self.ack_str + msg).encode('utf-8')).hexdigest()

        self.msg = msg

        self.packet = self.size_str + self.seq_num_str + self.ack_str + self.checksum + self.msg

        return self.packet


    def split(self, packet):

        limits = [self.size_len,
                  self.size_len + self.seq_num_len,
                  self.size_len + self.seq_num_len + 1,
                  self.size_len + self.seq_num_len + 1 + self.checksum_len]

        size_str = packet[0 : limits[0]]
        seq_num_str = packet[limits[0] : limits[1]]
        ack_str = packet[limits[1] : limits[2]]
        checksum = packet[limits[2] : limits[3]]
        msg = packet[limits[3] : ]

        return size_str, seq_num_str, ack_str, checksum, msg


    def corrupt(self, packet):

        size_str, seq_num_str, ack_str, checksum, msg = self.split(packet)

        computed = hashlib.md5(str(size_str + seq_num_str + ack_str + msg).encode('utf-8')).hexdigest()

        return checksum != computed


    def decode(self, packet):

        if self.corrupt(packet):
            raise RuntimeError('Cannot extract packet: it is corrupted')

        size_str, seq_num_str, ack_str, checksum, msg = self.split(packet)

        self.size = int(size_str)
        self.size_str = size_str

        self.seq_num = int(seq_num_str)
        self.seq_num_str = seq_num_str

        self.ack_str = ack_str
        self.ack = int(ack_str)

        self.checksum = checksum

        self.msg = msg

        self.packet = packet

        return size_str, seq_num_str, ack_str, checksum, msg


    def get_ack(self):
        if self.ack == Packet.ACK:
            return self.seq_num

        return -1

class Transport:
    packet_len = 5 # bytes per packet

    # split message into fragments, which are fixed length packets of len={packet_len}
    def split(self, text):

        msgs = []
        while( len(text) >= self.packet_len ):
            msg = text[:self.packet_len]
            msgs.append(msg)
            text = text[self.packet_len:]

        if (len(text)):
            msgs.append(text)

        return msgs


    def join(self, msgs):
        text = ""

        for fragment in msgs:
            text += fragment

        return text


    # queue a message and return right away, the protocol thread sends it
    def send(self, rdt, text):

        msgs = self.split(text)

        rdt.send(msgs)


    # wait for the next complete message
    # returns None if the connection was closed or went silent
    def receive(self, rdt):

        msgs = rdt.receive()

        if msgs is None:
            return None

        return self.join(msgs)

# Full duplex: each endpoint has one protocol thread (see `run`) that owns both
# a sending and a receiving window, so data flows in both directions at once.
# The two directions are independent: each has its own sequence numbers.
class RDT:

    window_len = 4
    modulo = window_len*2

    # packet timeout (s)
    timeout = 1

    # if nothing arrives for this many seconds, stop waiting for the peer
    connection_timeout = 30

    # pause (s) when there is nothing to do, so waiting doesn't use 100% CPU
    idle_sleep = 0.001

    thread = None

    def __init__(self, role_str, server_str, port, sender_metrics, receiver_metrics, loss=None, corrupt=None, reorder=None):
        self.sender_metrics = sender_metrics
        self.receiver_metrics = receiver_metrics
        self.network = network.NetworkLayer(role_str, server_str, port, loss, corrupt, reorder)
        self.reset_state()
        self.start()

    # Sequence numbers continue from one message to the next instead of restarting at 0.
    # Because the modulo is twice the window length, a late copy of a packet from an
    # earlier message always falls in the receiver's `behind` set and is never taken as new data.
    def reset_state(self):
        # protects everything below, notified when a message is complete or an ACK arrives
        self.cond = threading.Condition()

        self.stop = False        # disconnect() asks the protocol thread to stop
        self.finished = False    # the protocol thread has stopped
        self.error = None        # exception that stopped the protocol thread, if any

        # bytes read from the network that don't form a complete packet yet,
        # kept between reads because TCP may split a packet across them
        self.rx_buffer = ""

        # last time anything was read from the network
        self.last_read = time.time()

        # sender: fragments and FINs waiting for room in the window
        self.send_queue = collections.deque()

        # sender: next sequence number to use, and the oldest unacknowledged one
        self.send_next = 0
        self.send_base = 0

        # send_window = {seq : [wasACK?, packet, timer] }
        self.send_window = {}

        # receiver: base = oldest sequence number not yet received
        # behind = [base-N, base-1], window_range = [base, base+N-1] (modular arithmetic)
        # because the modulo is twice the window length, the two sets are complementary
        self.recv_base = 0
        self.recv_behind = set(range(self.window_len, self.modulo))
        self.recv_window_range = set(range(0, self.window_len))

        # {seq : packet}, buffers out-of-order packets inside recv_window_range
        self.recv_window = {}

        # fragments of the message being received (its FIN hasn't arrived yet)
        self.recv_msgs = []

        # complete messages, each a list of fragments, waiting for receive()
        self.received = collections.deque()

    def start(self):
        self.thread = threading.Thread(name='Protocol', target=self.run, daemon=True)
        self.thread.start()

    def disconnect(self):
        self.stop = True
        if self.thread:
            self.thread.join()
        self.network.disconnect()

    # whether the peer closed the connection
    def closed(self):
        return self.network.closed

    # ---------------- called by the application ----------------

    # queue a message: its fragments followed by the FIN that marks its end
    def send(self, msgs):
        with self.cond:
            self.send_queue.extend((msg, Packet.DATA) for msg in msgs)
            self.send_queue.append(("", Packet.FIN))

    # wait for the next complete message and return its fragments
    # returns None if the connection was closed or went silent
    def receive(self):
        with self.cond:
            self.wait_until(lambda: self.received)
            return self.received.popleft() if self.received else None

    # wait until the peer has ACKed everything that was sent
    # returns whether it did
    def flush(self):
        done = lambda: not self.send_queue and not self.send_window
        with self.cond:
            self.wait_until(done)
            return done()

    # wait for a condition, giving up if the protocol thread stopped or the peer went silent
    def wait_until(self, condition):
        start = time.time()

        while not condition():

            if self.error:
                raise self.error

            if self.finished:
                return

            if max(start, self.last_read) + self.connection_timeout < time.time():
                debug_log("##################################################")
                debug_log(f"Time limit: nothing received for {self.connection_timeout} s, at {format_time(time.time_ns())}")
                debug_log("##################################################\n")
                return

            # wake up often so Ctrl+C works and the time limit is checked
            self.cond.wait(0.1)

    # ---------------- protocol thread ----------------

    def run(self):
        debug_log("--------------------------------------------------")
        debug_log(f"Connection started at {format_time(time.time_ns())}")
        debug_log("--------------------------------------------------\n")

        try:
            while not self.stop:

                # check before reading, so data that arrived before the close is still processed
                closed = self.closed()

                if not self.step():
                    if closed:
                        debug_log("##################################################")
                        debug_log(f"Connection closed by the peer at {format_time(time.time_ns())}")
                        debug_log("##################################################\n")
                        break

                    time.sleep(self.idle_sleep)

        except Exception as err:
            self.error = err

        finally:
            with self.cond:
                self.finished = True
                self.cond.notify_all()

    # one round of both directions, returns whether there was anything to send or read
    def step(self):
        with self.cond:
            sent = self.send_new_packets()
            self.resend_expired_packets()

            got_data, packets = self.read_packets()
            for received in packets:
                self.handle_packet(received)

            return sent or got_data

    # read from the network and extract every complete packet in the buffer
    # returns (whether anything was read, list of raw packets)
    def read_packets(self):
        received = self.network.udt_receive()
        if received:
            self.rx_buffer += received
            self.last_read = time.time()

        min_len = Packet.size_len + Packet.seq_num_len + 1 + Packet.checksum_len

        packets = []
        while len(self.rx_buffer) >= Packet.size_len:

            # length attribute may be corrupted
            try:
                length = int(self.rx_buffer[0:Packet.size_len])
            except ValueError:
                length = -1

            # invalid length: packet boundaries are lost, drop the buffer to resync
            if length < min_len:
                debug_log(f"Received: CORRUPTED length, dropping buffer, at {format_time(time.time_ns())}" + f"\n\t {self.rx_buffer}\n")
                self.receiver_metrics.add_corrupted_received()
                self.rx_buffer = ""
                break

            # incomplete packet: wait for the rest of it on the next read
            if len(self.rx_buffer) < length:
                break

            packets.append(self.rx_buffer[0:length])
            self.rx_buffer = self.rx_buffer[length:]

        return bool(received), packets

    # ACKs belong to our sending direction, data and FINs to the receiving one
    def handle_packet(self, received):
        p = Packet()

        if p.corrupt(received):
            debug_log(f"Received: CORRUPTED packet, at {format_time(time.time_ns())}" + f"\n\t {received}\n")
            if self.looks_like_ack(received):
                self.sender_metrics.add_corrupted_received()
            else:
                self.receiver_metrics.add_corrupted_received()
            return

        p.decode(received)

        if p.ack == Packet.ACK:
            self.handle_ack(p)
        else:
            self.handle_data(p)

    # which direction a corrupted packet belongs to, only used for the metrics
    # if the ackFlag itself was corrupted, a packet without data is taken as an ACK
    def looks_like_ack(self, received):
        _, _, ack_str, _, msg = Packet().split(received)
        if ack_str in (str(Packet.DATA), str(Packet.FIN)):
            return False
        return ack_str == str(Packet.ACK) or msg == ""

    # ---------------- sending direction ----------------

    # send queued packets while there is room in the window
    # returns whether anything was sent
    def send_new_packets(self):
        sent = False

        while len(self.send_queue) > 0 and len(self.send_window) < self.window_len:

            msg, kind = self.send_queue.popleft()
            seq = self.send_next
            p = Packet()
            p.code(seq, msg, kind)
            self.send_window[seq] = [False, p, time.time()]
            self.send_next = (seq + 1) % self.modulo
            debug_log(f"First time: Sending {'FIN' if kind == Packet.FIN else 'packet'} {seq}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")

            self.sender_metrics.set_start()
            self.network.udt_send(p.packet)
            self.sender_metrics.add_packet_sent(p)
            self.sender_metrics.add_packet_first_sent(p)
            self.sender_metrics.set_end()
            sent = True

        return sent

    # resend only the packets whose timer expired
    def resend_expired_packets(self):
        for seq, entry in self.send_window.items():
            wasAck, p, timer = entry

            if (not wasAck) and (timer + self.timeout < time.time()):
                debug_log(f"Timeout: Resend packet {seq}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")
                entry[2] = time.time()
                self.network.udt_send(p.packet)
                self.sender_metrics.add_packet_sent(p)
                self.sender_metrics.add_retransmission_sent(p)
                self.sender_metrics.set_end()

    def handle_ack(self, p):
        debug_log(f"Received ACK: Reading non corrupted ACK {p.seq_num}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")

        if p.seq_num not in self.send_window:
            return

        self.send_window[p.seq_num][0] = True

        # slide window
        while self.send_base in self.send_window and self.send_window[self.send_base][0]:
            self.send_window.pop(self.send_base)
            self.send_base = (self.send_base + 1) % self.modulo

        self.cond.notify_all()

    # ---------------- receiving direction ----------------

    def send_ack(self, num):
        a = Packet()
        a.code(num, "", Packet.ACK)
        debug_log(f"Sending ACK: for packet {num}, at {format_time(time.time_ns())}" + f"\n\t {a.packet}\n")
        self.receiver_metrics.set_start()
        self.network.udt_send(a.packet)
        self.receiver_metrics.add_packet_sent(a)
        self.receiver_metrics.set_end()

    def handle_data(self, p):
        num = p.seq_num
        debug_log(f"Received: Reading non corrupted {'FIN' if p.ack == Packet.FIN else 'packet'} {num}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")

        # already delivered, but our ACK may have been lost: ACK again, don't buffer
        if num in self.recv_behind:
            self.send_ack(num)

        # inside the window: ACK and buffer it
        elif num in self.recv_window_range:
            self.send_ack(num)
            self.recv_window[num] = p
            self.deliver()

    # take consecutive buffered packets in order and slide the window
    # a message is only handed over once its FIN arrives, so it is never incomplete
    def deliver(self):

        while (self.recv_base in self.recv_window):
            idx = self.recv_base
            p = self.recv_window.pop(idx)

            self.recv_behind.remove((idx-self.window_len+self.modulo) % self.modulo)
            self.recv_behind.add(idx)
            self.recv_window_range.remove(idx)
            self.recv_window_range.add((idx+self.window_len) % self.modulo)

            self.recv_base = (idx + 1) % self.modulo

            if p.ack == Packet.FIN:
                debug_log("##################################################")
                debug_log(f"Received FIN: message complete at {format_time(time.time_ns())}")
                debug_log("##################################################\n")
                self.received.append(self.recv_msgs)
                self.recv_msgs = []
                self.cond.notify_all()
            else:
                self.recv_msgs.append(p.msg)
