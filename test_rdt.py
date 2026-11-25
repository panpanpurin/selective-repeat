import time
import unittest

import rdt
from rdt import RDT, Packet

rdt.debug = False


class FakeNetwork:
    """Hands queued raw packets to the receiver one per read, records what is sent."""
    def __init__(self, incoming):
        self.incoming = list(incoming)
        self.sent = []

    def udt_receive(self):
        return self.incoming.pop(0) if self.incoming else ""

    def udt_send(self, msg):
        self.sent.append(msg)


class FakePeer:
    """A receiver that ACKs every data packet; can drop the FIN's ACK and reply once it gets the FIN."""
    def __init__(self, ack_fin=True, reply=None):
        self.ack_fin = ack_fin
        self.reply = reply or []
        self.incoming = ""
        self.sent = []

    def udt_send(self, msg):
        self.sent.append(msg)
        p = Packet()
        p.decode(msg)
        if p.ack == Packet.DATA or (p.ack == Packet.FIN and self.ack_fin):
            self.incoming += ack(p.seq_num)
        if p.ack == Packet.FIN and self.reply:
            self.incoming += "".join(self.reply)
            self.reply = []

    def udt_receive(self):
        out, self.incoming = self.incoming, ""
        return out


class NullMetrics:
    def __getattr__(self, name):
        return lambda *a, **k: None


def make_rdt(net, window_len=4):
    r = RDT.__new__(RDT)
    r.window_len = window_len
    r.modulo = window_len * 2
    r.connection_timeout = 0.3
    r.network = net if not isinstance(net, list) else FakeNetwork(net)
    r.reset_state()
    return r


def data(seq, msg):
    return Packet().code(seq, msg, Packet.DATA)


def ack(seq):
    return Packet().code(seq, "", Packet.ACK)


def fin(seq):
    return Packet().code(seq, "", Packet.FIN)


def kinds(sent):
    out = []
    for raw in sent:
        p = Packet()
        p.decode(raw)
        out.append(({0: "D", 1: "A", 2: "F"}[p.ack], p.seq_num))
    return out


class ReceiverTests(unittest.TestCase):

    def test_stale_ack_is_not_taken_as_data(self):
        r = make_rdt([ack(0), data(0, "hello"), data(1, "world")])
        self.assertEqual(r.rdt_4_0_receive(NullMetrics()), ["hello", "world"])

    def test_in_order_default_window(self):
        frags = [f"m{i}" for i in range(10)]   # wraps past modulo 8
        r = make_rdt([data(i % 8, f) for i, f in enumerate(frags)])
        self.assertEqual(r.rdt_4_0_receive(NullMetrics()), frags)

    def test_out_of_order_is_reassembled(self):
        r = make_rdt([data(2, "c"), data(1, "b"), data(3, "d"), data(0, "a")])
        self.assertEqual(r.rdt_4_0_receive(NullMetrics()), ["a", "b", "c", "d"])

    def test_duplicate_is_reacked_not_redelivered(self):
        r = make_rdt([data(0, "a"), data(0, "a"), data(1, "b")])
        self.assertEqual(r.rdt_4_0_receive(NullMetrics()), ["a", "b"])
        self.assertEqual(len(r.network.sent), 3)   # every data packet got an ACK

    def test_other_window_lengths(self):
        for n in (2, 3, 5):
            with self.subTest(window_len=n):
                frags = [f"m{i}" for i in range(3 * n)]
                # deliver each window reversed to force buffering
                order = []
                for start in range(0, len(frags), n):
                    order += list(reversed(range(start, min(start + n, len(frags)))))
                pkts = [data(i % (2 * n), frags[i]) for i in order]
                r = make_rdt(pkts, window_len=n)
                self.assertEqual(r.rdt_4_0_receive(NullMetrics()), frags)

    def test_fin_ends_receive_without_waiting(self):
        r = make_rdt([data(0, "a"), fin(2), data(1, "b")])   # FIN arrives before the last data
        r.connection_timeout = 5
        start = time.time()
        self.assertEqual(r.rdt_4_0_receive(NullMetrics()), ["a", "b"])
        self.assertLess(time.time() - start, 0.5)
        self.assertIn(("A", 2), kinds(r.network.sent))   # the FIN itself is ACKed

    def test_text_fin_is_ordinary_data(self):
        r = make_rdt([data(0, "FIN"), fin(1)])
        self.assertEqual(r.rdt_4_0_receive(NullMetrics()), ["FIN"])

    def test_sequence_numbers_continue_across_messages(self):
        r = make_rdt([data(0, "a"), fin(1)])
        self.assertEqual(r.rdt_4_0_receive(NullMetrics()), ["a"])
        # next message starts at 2; a late copy of packet 0 must be re-ACKed, not delivered
        r.network = FakeNetwork([data(0, "a"), data(2, "b"), fin(3)])
        self.assertEqual(r.rdt_4_0_receive(NullMetrics()), ["b"])
        self.assertEqual(kinds(r.network.sent), [("A", 0), ("A", 2), ("A", 3)])


class SenderTests(unittest.TestCase):

    def test_sends_fin_and_finishes_when_acked(self):
        peer = FakePeer()
        r = make_rdt(peer)
        start = time.time()
        r.rdt_4_0_send(["a", "b", "c", "d", "e"], NullMetrics())
        self.assertLess(time.time() - start, 0.5)
        self.assertEqual(kinds(peer.sent), [("D", 0), ("D", 1), ("D", 2), ("D", 3), ("D", 4), ("F", 5)])
        self.assertEqual(r.send_next, 6)

    def test_lost_fin_ack_peer_data_ends_send_and_is_kept(self):
        # the peer never ACKs our FIN but starts its reply: that proves it got the FIN
        peer = FakePeer(ack_fin=False, reply=[data(0, "X"), data(1, "Y"), fin(2)])
        r = make_rdt(peer)
        r.timeout = 10   # no resend needed: the reply alone must end the send
        start = time.time()
        r.rdt_4_0_send(["a", "b"], NullMetrics())
        self.assertLess(time.time() - start, 0.5)
        self.assertEqual(r.rdt_4_0_receive(NullMetrics()), ["X", "Y"])

    def test_resent_old_packet_is_reacked_while_sending(self):
        # our ACK for the peer's last FIN was lost; it resends it while we are sending
        net = FakePeer()
        r = make_rdt(net)
        r.recv_behind = {4, 5, 6, 7}
        net.incoming = fin(7)
        r.rdt_4_0_send(["a"], NullMetrics())
        self.assertIn(("A", 7), kinds(net.sent))

    def test_gives_up_on_fin_after_max_retries(self):
        peer = FakePeer(ack_fin=False)
        r = make_rdt(peer)
        r.timeout = 0.02
        r.fin_max_retries = 3
        r.rdt_4_0_send(["a"], NullMetrics())
        self.assertEqual(kinds(peer.sent).count(("F", 1)), 4)   # first send + 3 resends


if __name__ == "__main__":
    unittest.main(verbosity=2)
