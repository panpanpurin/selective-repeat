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
