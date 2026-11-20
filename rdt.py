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
        if self.ack:
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

    # if the receiver gets no packets for this many seconds, it ends the connection
    connection_timeout = 5

    def __init__(self, role_str, server_str, port):
        self.network = network.NetworkLayer(role_str, server_str, port)

        # bytes read from the network that don't form a complete packet yet,
        # kept between reads because TCP may split a packet across them
        self.rx_buffer = ""

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

    def rdt_4_0_send(self, msgs, metrics):

        queue = list(msgs)

        # next sequence number [0 to 7] to be sent
        nxt = 0

        # sequence number of the oldest unacknowledged packet in the window
        base = 0

        # window = {idx : [wasACK?, packet, timer] }
        window = {}

        debug_log("--------------------------------------------------")
        debug_log(f"Started sending packets at {format_time(time.time_ns())}")
        debug_log("--------------------------------------------------\n")

        while(True):

            if (len(queue) == 0 and len(window) == 0):

                # wait {connection_timeout} seconds so the receiver times out before we move on
                time.sleep(self.connection_timeout)

                debug_log("##################################################")
                debug_log(f"Sent all packets, terminating at {format_time(time.time_ns())}")
                debug_log("##################################################\n")
                break

            # data to send and room in the window
            if ((len(queue) > 0) and (len(window) < self.window_len)):

                msg = queue.pop(0)
                p = Packet()
                p.code(nxt, msg, 0)
                window[nxt] = [False, p, time.time()]
                debug_log(f"First time: Sending packet {nxt}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")
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
                    debug_log(f"Timeout: Resend packet {seq}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")
                    window[seq][2] = time.time()
                    self.network.udt_send(p.packet)
                    metrics.add_packet_sent(p)
                    metrics.add_retransmission_sent(p)
                    metrics.set_end()

            _, packets = self.read_packets(metrics)

            for received in packets:

                p = Packet()
                if (not p.corrupt(received)):
                    p.decode(received)
                    ack_num = p.get_ack()
                    debug_log(f"Received ACK: Reading non corrupted ACK {ack_num}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")
                    if (ack_num != -1 and ack_num in window):
                        window[ack_num][0] = True

                        # slide window
                        if (ack_num == base):
                            idx = base
                            while(idx in window and window[idx][0]):
                                window.pop(idx)
                                idx = (idx + 1) % self.modulo
                            base = idx
                else:
                    debug_log(f"Received: CORRUPTED packet, at {format_time(time.time_ns())}" + f"\n\t {received}\n")
                    metrics.add_corrupted_received()

    def rdt_4_0_receive(self, metrics):

        # fragments delivered in order to the transport layer
        msgs = []

        # sequence number of the oldest packet not yet received
        base = 0

        # behind = [base-N, base-1], window_range = [base, base+N-1] (modular arithmetic)
        # because the modulo is twice the window length, the two sets are complementary
        behind = {4, 5, 6, 7}
        window_range = {0, 1, 2, 3}

        # window = {idx : packet}, buffers out-of-order packets inside window_range
        window = {}

        last_timer = time.time()

        debug_log("--------------------------------------------------")
        debug_log(f"Started receiving packets at {format_time(time.time_ns())}")
        debug_log("--------------------------------------------------\n")

        while(True):

            if (last_timer + self.connection_timeout < time.time()):
                debug_log("##################################################")
                debug_log(f"Time limit: Stopped receiving packets at {format_time(time.time_ns())}")
                debug_log("##################################################\n")
                break

            got_data, packets = self.read_packets(metrics)

            if (got_data):
                last_timer = time.time()

            for received in packets:

                p = Packet()
                if (not p.corrupt(received)):
                    p.decode(received)
                    num = p.seq_num
                    debug_log(f"Received: Reading non corrupted packet {num}, at {format_time(time.time_ns())}" + f"\n\t {p.packet}\n")

                    # already delivered, but our ACK may have been lost: ACK again, don't buffer
                    if num in behind:
                        a = Packet()
                        a.code(num, "", 1)
                        debug_log(f"Sending ACK: for packet {num}, at {format_time(time.time_ns())}" + f"\n\t {a.packet}\n")
                        self.network.udt_send(a.packet)
                        metrics.set_end()
                        metrics.set_start()
                        metrics.add_packet_sent(a)

                    # inside the window: ACK and buffer it
                    elif num in window_range:
                        a = Packet()
                        a.code(num, "", 1)
                        debug_log(f"Sending ACK: for packet {num}, at {format_time(time.time_ns())}" + f"\n\t {a.packet}\n")
                        self.network.udt_send(a.packet)
                        metrics.set_end()
                        metrics.set_start()
                        metrics.add_packet_sent(a)

                        window[num] = p

                        # deliver consecutive buffered packets and slide the window
                        if (num == base):
                            idx = base
                            while(idx in window):

                                if (window[idx].msg != "FIN"):
                                    msgs.append(window[idx].msg)
                                window.pop(idx)

                                behind.remove((idx-self.window_len+self.modulo) % self.modulo)
                                behind.add(idx)
                                window_range.remove(idx)
                                window_range.add((idx+self.window_len) % self.modulo)

                                idx = (idx + 1) % self.modulo

                            base = idx

                else:
                    debug_log(f"Received: CORRUPTED packet, at {format_time(time.time_ns())}" + f"\n\t {received}\n")
                    metrics.add_corrupted_received()
        return msgs

