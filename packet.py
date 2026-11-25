import hashlib

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
    header_len = size_len + seq_num_len + 1 + checksum_len

    size = None
    size_str = None
    seq_num = None
    seq_num_str = None
    ack = None
    ack_str = None
    checksum = None
    msg = None
    packet = None

    def code(self, seq_num, msg, is_ack=0):

        self.size = self.header_len + len(msg)
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
                  self.header_len]

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
