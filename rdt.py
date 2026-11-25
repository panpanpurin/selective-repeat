import collections
import threading
import time
from log import debug_log, format_time
from network import NetworkLayer
from packet import Packet

# Full duplex: each endpoint has one protocol thread (see `run`) that owns both
# a sending and a receiving window, so data flows in both directions at once.
# The two directions are independent: each has its own sequence numbers.
class RDT:

    window_len = 4

    # Twice the window length is only enough if packets are never reordered. The network
    # may release a held back packet after a newer one, when the window has already moved
    # past it, so a larger sequence space keeps old numbers from being taken as new ones.
    modulo = window_len*4

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
        self.network = NetworkLayer(role_str, server_str, port, loss, corrupt, reorder)
        self.reset_state()
        self.start()

    # Sequence numbers continue from one message to the next instead of restarting at 0.
    # A late copy of a packet from an earlier message falls outside the receiver's window,
    # so it's ACKed again but never taken as new data.
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

        # receiver: oldest sequence number not yet received
        # the window is [base, base+N-1] (modular arithmetic)
        self.recv_base = 0

        # {seq : packet}, buffers out-of-order packets inside the window
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

        packets = []
        while len(self.rx_buffer) >= Packet.size_len:

            # length attribute may be corrupted
            try:
                length = int(self.rx_buffer[0:Packet.size_len])
            except ValueError:
                length = -1

            # invalid length: packet boundaries are lost, drop the buffer to resync
            if length < Packet.header_len:
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

        # outside the window means already delivered, but our ACK may have been lost: ACK again
        self.send_ack(num)

        # inside the window: buffer it
        if (num - self.recv_base) % self.modulo < self.window_len:
            self.recv_window[num] = p
            self.deliver()

    # take consecutive buffered packets in order and slide the window
    # a message is only handed over once its FIN arrives, so it is never incomplete
    def deliver(self):

        while (self.recv_base in self.recv_window):
            p = self.recv_window.pop(self.recv_base)
            self.recv_base = (self.recv_base + 1) % self.modulo

            if p.ack == Packet.FIN:
                debug_log("##################################################")
                debug_log(f"Received FIN: message complete at {format_time(time.time_ns())}")
                debug_log("##################################################\n")
                self.received.append(self.recv_msgs)
                self.recv_msgs = []
                self.cond.notify_all()
            else:
                self.recv_msgs.append(p.msg)
