import network
import time
import hashlib

debug = True


def debug_log(message):
    if debug:
        print(message)

def format_time(val):
    f_time = time.strftime("%d-%m-%Y %H:%M:%S", time.gmtime(val/1000000000 - 3*60*60))
    us = (val//1000) % 1000000
    f_time += f".{us:3d} us"
    return f_time

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


    def send(self, rdt, text, metrics):

        msgs = self.split(text)

        rdt.rdt_4_0_send(msgs, metrics)


    def receive(self, rdt, metrics):

        msgs = rdt.rdt_4_0_receive(metrics)

        if msgs == []:
            return None

        text = self.join(msgs)

        return text

class RDT:

    window_len = 4
    modulo = window_len*2

    # packet timeout (s)
    timeout = 1

    # if the receiver gets no packets for this many seconds, it stops waiting
    connection_timeout = 5

    # FIN resends without an ACK before the sender assumes the receiver is done and gone
    fin_max_retries = 5

    def __init__(self, role_str, server_str, port, loss=None, corrupt=None, reorder=None):
        self.network = network.NetworkLayer(role_str, server_str, port, loss, corrupt, reorder)
        self.reset_state()

    # Sequence numbers continue from one message to the next instead of restarting at 0.
    # Because the modulo is twice the window length, a late copy of a packet from an
    # earlier message always falls in the receiver's `behind` set and is never taken as new data.
    def reset_state(self):
        # bytes read from the network that don't form a complete packet yet,
        # kept between reads because TCP may split a packet across them
        self.rx_buffer = ""

        # raw packets read while sending that belong to the next receive
        self.pending = []

        # sender: next sequence number to use
        self.send_next = 0

        # receiver: base = oldest sequence number not yet received
        # behind = [base-N, base-1], window_range = [base, base+N-1] (modular arithmetic)
        # because the modulo is twice the window length, the two sets are complementary
        self.recv_base = 0
        self.recv_behind = set(range(self.window_len, self.modulo))
        self.recv_window_range = set(range(0, self.window_len))

        # {seq : packet}, buffers out-of-order packets inside recv_window_range
        self.recv_window = {}

    def disconnect(self):
        self.network.disconnect()

    # read from the network and extract every complete packet in the buffer
    # returns (whether anything was read, list of raw packets)
    def read_packets(self, metrics):
        received = self.network.udt_receive()
        if received:
            self.rx_buffer += received

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
                metrics.add_corrupted_received()
                self.rx_buffer = ""
                break

            # incomplete packet: wait for the rest of it on the next read
            if len(self.rx_buffer) < length:
                break

            packets.append(self.rx_buffer[0:length])
            self.rx_buffer = self.rx_buffer[length:]

        return bool(received), packets

    def send_ack(self, num, metrics):
        a = Packet()
        a.code(num, "", Packet.ACK)
        debug_log(f"Sending ACK: for packet {num}, at {format_time(time.time_ns())}" + f"\n\t {a.packet}\n")
        self.network.udt_send(a.packet)
        metrics.set_end()
        metrics.set_start()
        metrics.add_packet_sent(a)

    def rdt_4_0_send(self, msgs, metrics):

        # fragments followed by the FIN that marks the end of the message
        queue = [(msg, Packet.DATA) for msg in msgs] + [("", Packet.FIN)]

        # next sequence number [0 to modulo-1] to be sent
        nxt = self.send_next

        # sequence number of the oldest unacknowledged packet in the window
        base = self.send_next

        # window = {idx : [wasACK?, packet, timer] }
        window = {}

        fin_retries = 0
        done = False

        debug_log("--------------------------------------------------")
        debug_log(f"Started sending packets at {format_time(time.time_ns())}")
        debug_log("--------------------------------------------------\n")

        while(not done):

            if (len(queue) == 0 and len(window) == 0):
                break

            # data to send and room in the window
            if ((len(queue) > 0) and (len(window) < self.window_len)):

                msg, kind = queue.pop(0)
                p = Packet()
                p.code(nxt, msg, kind)
                window[nxt] = [False, p, time.time()]
                debug_log(f"First time: Sending {'FIN' if kind == Packet.FIN else 'packet'} {nxt}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")
                nxt = (nxt + 1) % self.modulo

                self.network.udt_send(p.packet)
                metrics.add_packet_sent(p)
                metrics.add_packet_first_sent(p)
                metrics.set_end()
                metrics.set_start()

            # resend only the packets whose timer expired
            for seq in window.keys():
                wasAck, p, timer = window[seq]

                if (not wasAck) and (timer + self.timeout < time.time()):

                    # the FIN's ACK may be lost after the receiver has left for good
                    # (e.g. the last reply of a session): don't resend it forever
                    if p.ack == Packet.FIN:
                        fin_retries += 1
                        if fin_retries > self.fin_max_retries:
                            debug_log("##################################################")
                            debug_log(f"FIN not ACKed after {self.fin_max_retries} resends, assuming the receiver is done, at {format_time(time.time_ns())}")
                            debug_log("##################################################\n")
                            done = True
                            break

                    debug_log(f"Timeout: Resend packet {seq}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")
                    window[seq][2] = time.time()
                    self.network.udt_send(p.packet)
                    metrics.add_packet_sent(p)
                    metrics.add_retransmission_sent(p)
                    metrics.set_end()

            if done:
                break

            _, packets = self.read_packets(metrics)

            for i, received in enumerate(packets):

                p = Packet()
                if p.corrupt(received):
                    debug_log(f"Received: CORRUPTED packet, at {format_time(time.time_ns())}" + f"\n\t {received}\n")
                    metrics.add_corrupted_received()
                    continue

                p.decode(received)

                if p.ack == Packet.ACK:
                    ack_num = p.seq_num
                    debug_log(f"Received ACK: Reading non corrupted ACK {ack_num}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")
                    if (ack_num in window):
                        window[ack_num][0] = True

                        # slide window
                        if (ack_num == base):
                            idx = base
                            while(idx in window and window[idx][0]):
                                window.pop(idx)
                                idx = (idx + 1) % self.modulo
                            base = idx

                # a packet we already delivered: our ACK for it was lost, so ACK it again
                elif p.seq_num in self.recv_behind:
                    self.send_ack(p.seq_num, metrics)

                # new data from the peer: it only starts sending after it received our
                # whole message, FIN included, so we are done even if ACKs are missing.
                # Keep this packet and the rest for the next receive.
                else:
                    if len(window) > 0:
                        debug_log("##################################################")
                        debug_log(f"Received new data while waiting for ACKs: the receiver got our FIN, terminating at {format_time(time.time_ns())}")
                        debug_log("##################################################\n")
                    self.pending = packets[i:]
                    done = True
                    break

        if len(window) == 0:
            debug_log("##################################################")
            debug_log(f"Sent all packets and the FIN was ACKed, terminating at {format_time(time.time_ns())}")
            debug_log("##################################################\n")

        self.send_next = nxt

    # deliver consecutive buffered packets in order and slide the window
    # stops after a FIN so packets of a following message stay buffered
    # returns whether the FIN was delivered
    def deliver(self, msgs):
        idx = self.recv_base
        got_fin = False

        while (idx in self.recv_window):
            p = self.recv_window.pop(idx)

            self.recv_behind.remove((idx-self.window_len+self.modulo) % self.modulo)
            self.recv_behind.add(idx)
            self.recv_window_range.remove(idx)
            self.recv_window_range.add((idx+self.window_len) % self.modulo)

            idx = (idx + 1) % self.modulo

            if p.ack == Packet.FIN:
                got_fin = True
                break

            msgs.append(p.msg)

        self.recv_base = idx
        return got_fin

    def rdt_4_0_receive(self, metrics):

        # fragments delivered in order to the transport layer
        msgs = []

        last_timer = time.time()

        debug_log("--------------------------------------------------")
        debug_log(f"Started receiving packets at {format_time(time.time_ns())}")
        debug_log("--------------------------------------------------\n")

        got_fin = self.deliver(msgs)

        while(not got_fin):

            if (last_timer + self.connection_timeout < time.time()):
                debug_log("##################################################")
                debug_log(f"Time limit: Stopped receiving packets at {format_time(time.time_ns())}")
                debug_log("##################################################\n")
                break

            # packets the sender already read for us come first
            if self.pending:
                packets, self.pending = self.pending, []
            else:
                got_data, packets = self.read_packets(metrics)
                if (got_data):
                    last_timer = time.time()

            for received in packets:

                p = Packet()
                if p.corrupt(received):
                    debug_log(f"Received: CORRUPTED packet, at {format_time(time.time_ns())}" + f"\n\t {received}\n")
                    metrics.add_corrupted_received()
                    continue

                p.decode(received)

                # the receiver only expects data; a stray ACK is not a data packet
                if p.ack == Packet.ACK:
                    debug_log(f"Received: ignoring ACK {p.seq_num}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")
                    continue

                num = p.seq_num
                debug_log(f"Received: Reading non corrupted {'FIN' if p.ack == Packet.FIN else 'packet'} {num}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")

                # already delivered, but our ACK may have been lost: ACK again, don't buffer
                if num in self.recv_behind:
                    self.send_ack(num, metrics)

                # inside the window: ACK and buffer it
                elif num in self.recv_window_range:
                    self.send_ack(num, metrics)
                    self.recv_window[num] = p

                    if self.deliver(msgs):
                        got_fin = True

        if got_fin:
            debug_log("##################################################")
            debug_log(f"Received FIN: message complete at {format_time(time.time_ns())}")
            debug_log("##################################################\n")

        return msgs
