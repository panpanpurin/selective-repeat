import argparse
import contextlib
import io
import os
import random
import socket
import struct
import subprocess
import sys
import threading
import time
import unittest

import log
from network import NetworkLayer
from options import add_options
from packet import Packet
from rdt import RDT
from transport import Transport

log.debug = False


class FakeNetwork:
    """Hands queued raw packets over one per read, records what is sent."""
    closed = False

    def __init__(self, incoming=()):
        self.incoming = list(incoming)
        self.sent = []

    def udt_receive(self):
        return self.incoming.pop(0) if self.incoming else ""

    def udt_send(self, msg):
        self.sent.append(msg)

    def disconnect(self):
        pass


class AckingPeer(FakeNetwork):
    """A receiver that ACKs every data packet and FIN."""
    def udt_send(self, msg):
        self.sent.append(msg)
        p = Packet()
        p.decode(msg)
        if p.ack != Packet.ACK:
            self.incoming.append(ack(p.seq_num))


class LinkedNetwork:
    """Two of these, linked, form an in-memory connection that can drop, corrupt and reorder packets."""
    closed = False

    def __init__(self, seed, error=0.0, delay=0.0):
        self.random = random.Random(seed)
        self.error = error
        self.delay = delay
        self.lock = threading.Lock()
        self.incoming = []   # (time it becomes readable, packet)
        self.held = None
        self.sent = []
        self.peer = None

    def udt_send(self, msg):
        self.sent.append((time.time(), msg))
        if self.random.random() < self.error:
            return
        if self.random.random() < self.error:
            msg = msg[:20] + "X" + msg[21:]
        if self.held is not None:
            msg, self.held = msg + self.held, None
        elif self.random.random() < self.error:
            self.held = msg
            return
        with self.peer.lock:
            self.peer.incoming.append((time.time() + self.delay, msg))

    def udt_receive(self):
        with self.lock:
            ready = [msg for at, msg in self.incoming if at <= time.time()]
            self.incoming = self.incoming[len(ready):]
        return "".join(ready)

    def disconnect(self):
        self.peer.closed = True


class CountingMetrics:
    """Counts the calls to every metrics method."""
    def __init__(self):
        self.calls = {}

    def __getattr__(self, name):
        def call(*args):
            self.calls[name] = self.calls.get(name, 0) + 1
        return call


def make_rdt(net, window_len=4):
    r = RDT.__new__(RDT)
    r.window_len = window_len
    r.sender_metrics = CountingMetrics()
    r.receiver_metrics = CountingMetrics()
    r.network = net if not isinstance(net, list) else FakeNetwork(net)
    r.reset_state()
    return r


def run_steps(r, count=60):
    for _ in range(count):
        r.step()


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

    def receive(self, packets, window_len=4):
        r = make_rdt(packets, window_len)
        run_steps(r, len(packets))
        return r

    def test_in_order_default_window(self):
        frags = [f"m{i}" for i in range(20)]   # wraps past modulo 16
        r = self.receive([data(i % 16, f) for i, f in enumerate(frags)] + [fin(4)])
        self.assertEqual(list(r.received), [frags])

    def test_out_of_order_is_reassembled(self):
        r = self.receive([data(2, "c"), data(1, "b"), fin(3), data(0, "a")])
        self.assertEqual(list(r.received), [["a", "b", "c"]])

    def test_duplicate_is_reacked_not_redelivered(self):
        r = self.receive([data(0, "a"), data(0, "a"), data(1, "b"), fin(2)])
        self.assertEqual(list(r.received), [["a", "b"]])
        self.assertEqual(kinds(r.network.sent), [("A", 0), ("A", 0), ("A", 1), ("A", 2)])

    def test_other_window_lengths(self):
        for n in (2, 3, 5):
            with self.subTest(window_len=n):
                frags = [f"m{i}" for i in range(6 * n)]
                # deliver each window reversed to force buffering
                order = []
                for start in range(0, len(frags), n):
                    order += list(reversed(range(start, min(start + n, len(frags)))))
                packets = [data(i % (4 * n), frags[i]) for i in order] + [fin(2 * n)]
                r = self.receive(packets, window_len=n)
                self.assertEqual(list(r.received), [frags])

    def test_text_fin_is_ordinary_data(self):
        r = self.receive([data(0, "FIN"), fin(1)])
        self.assertEqual(list(r.received), [["FIN"]])

    def test_message_without_its_fin_is_not_delivered(self):
        r = self.receive([data(0, "a"), data(1, "b")])
        r.connection_timeout = 0.1
        self.assertIsNone(r.receive())   # gives up, but must not return part of a message
        r.network.incoming = [fin(2)]
        run_steps(r, 1)
        self.assertEqual(r.receive(), ["a", "b"])

    def test_several_messages_in_a_row(self):
        r = self.receive([data(0, "a"), data(2, "b"), fin(3), fin(1)])
        self.assertEqual(list(r.received), [["a"], ["b"]])

    def test_empty_message(self):
        r = self.receive([fin(0)])
        self.assertEqual(Transport().receive(r), "")

    def test_late_copy_released_after_a_newer_packet_is_not_new_data(self):
        # the copy of 0 arrives right after 4, when the window is already [5, 8]
        late = data(4, "m4") + data(0, "m0")
        packets = [data(i, f"m{i}") for i in range(4)] + [late] + [data(i, f"m{i}") for i in range(5, 9)] + [fin(9)]
        r = self.receive(packets)
        self.assertEqual(list(r.received), [[f"m{i}" for i in range(9)]])

    def test_old_packet_of_earlier_message_is_only_reacked(self):
        r = self.receive([data(0, "a"), fin(1), data(0, "a"), data(2, "b"), fin(3)])
        self.assertEqual(list(r.received), [["a"], ["b"]])
        self.assertEqual(kinds(r.network.sent), [("A", 0), ("A", 1), ("A", 0), ("A", 2), ("A", 3)])


class SenderTests(unittest.TestCase):

    def test_sends_fin_and_finishes_when_acked(self):
        r = make_rdt(AckingPeer())
        r.send(["a", "b", "c", "d", "e"])
        run_steps(r)
        self.assertTrue(r.flush())
        self.assertEqual(kinds(r.network.sent), [("D", 0), ("D", 1), ("D", 2), ("D", 3), ("D", 4), ("F", 5)])
        self.assertEqual(r.send_next, 6)

    def test_sequence_numbers_continue_across_messages(self):
        r = make_rdt(AckingPeer())
        r.send(["a"])
        r.send(["b"])
        run_steps(r)
        self.assertEqual(kinds(r.network.sent), [("D", 0), ("F", 1), ("D", 2), ("F", 3)])

    def test_window_limits_packets_in_flight(self):
        r = make_rdt([])
        r.send(["a"] * 10)
        run_steps(r)
        self.assertEqual(kinds(r.network.sent), [("D", 0), ("D", 1), ("D", 2), ("D", 3)])

    def test_selective_repeat_resends_only_what_is_missing(self):
        r = make_rdt([])
        r.timeout = 0.05
        r.send(["a", "b", "c"])
        r.network.incoming = [ack(0), ack(2), ack(3)]   # 1 is lost
        run_steps(r, 5)
        time.sleep(0.06)
        run_steps(r, 1)
        self.assertEqual(kinds(r.network.sent)[4:], [("D", 1)])
        r.network.incoming = [ack(1)]
        run_steps(r, 1)
        self.assertTrue(r.flush())
        self.assertEqual(r.sender_metrics.calls["add_retransmission_sent"], 1)

    def test_late_ack_released_after_a_newer_one_is_not_a_new_ack(self):
        r = make_rdt([ack(0), ack(1), ack(2), ack(3), ack(4), ack(0)])
        r.send([f"m{i}" for i in range(20)])
        run_steps(r, 8)
        # window is [5, 8]: none of them was ACKed
        self.assertEqual({seq: entry[0] for seq, entry in r.send_window.items()}, {5: False, 6: False, 7: False, 8: False})

    def test_flush_gives_up_on_silent_peer(self):
        r = make_rdt([])
        r.connection_timeout = 0.1
        r.send(["a"])
        run_steps(r)
        self.assertFalse(r.flush())


class DuplexTests(unittest.TestCase):

    def test_data_is_received_while_own_data_is_unacked(self):
        r = make_rdt([data(0, "x"), fin(1), ack(0), ack(1), ack(2)])
        r.send(["a", "b"])
        run_steps(r, 2)
        self.assertEqual(list(r.received), [["x"]])
        self.assertEqual(len(r.send_window), 3)   # peer data is not taken as an ACK of ours
        run_steps(r, 3)
        self.assertTrue(r.flush())

    def test_corrupted_packets_are_counted_in_their_direction(self):
        bad = lambda raw, at: raw[:at] + "X" + raw[at + 1:]
        r = make_rdt([bad(ack(0), 20), bad(data(0, "hello"), 20), bad(fin(0), 20), bad(ack(0), 16), bad(data(0, "hello"), 16)])
        run_steps(r, 5)
        self.assertEqual(r.sender_metrics.calls["add_corrupted_received"], 2)
        self.assertEqual(r.receiver_metrics.calls["add_corrupted_received"], 3)

    def linked_pair(self, seed, error=0.0, delay=0.0, window_lens=(4, 4)):
        nets = [LinkedNetwork(seed, error, delay), LinkedNetwork(seed + 1, error, delay)]
        nets[0].peer, nets[1].peer = nets[1], nets[0]
        pair = [make_rdt(net, n) for net, n in zip(nets, window_lens)]
        for r in pair:
            r.timeout = 0.03
            r.connection_timeout = 10
            r.start()
            self.addCleanup(r.disconnect)
        return pair

    def test_both_sides_send_at_the_same_time(self):
        a, b = self.linked_pair(1, delay=0.01)
        t = Transport()
        for i in range(3):
            t.send(a, f"from a number {i} " * 5)
            t.send(b, f"from b number {i} " * 5)
        for i in range(3):
            self.assertEqual(t.receive(a), f"from b number {i} " * 5)
            self.assertEqual(t.receive(b), f"from a number {i} " * 5)
        self.assertTrue(a.flush() and b.flush())

        # both directions carried data during the same period
        spans = []
        for r in (a, b):
            times = [at for at, raw in r.network.sent if kinds([raw])[0][0] == "D"]
            spans.append((min(times), max(times)))
        overlap = min(spans[0][1], spans[1][1]) - max(spans[0][0], spans[1][0])
        self.assertGreater(overlap, 0.8 * (spans[0][1] - spans[0][0]))

    def test_other_window_lengths_survive_errors(self):
        for n in (1, 2, 16):
            with self.subTest(window_len=n):
                a, b = self.linked_pair(n, error=0.2, window_lens=(n, n))
                t = Transport()
                texts = [f"message {i} with some text in it" for i in range(3)]
                for text in texts:
                    t.send(a, text)
                    t.send(b, text)
                self.assertEqual([t.receive(b) for _ in texts], texts)
                self.assertEqual([t.receive(a) for _ in texts], texts)

    def test_different_windows_give_an_error(self):
        a, b = self.linked_pair(1, window_lens=(8, 2))
        Transport().send(a, "long enough to use many sequence numbers " * 3)
        with self.assertRaisesRegex(RuntimeError, "same --window"):
            b.receive()

    def test_both_directions_survive_errors(self):
        for seed in range(0, 20, 2):
            with self.subTest(seed=seed):
                a, b = self.linked_pair(seed, error=0.25)
                t = Transport()
                to_b = [f"a{i} says something long enough {seed}" for i in range(4)]
                to_a = [f"b{i} answers with other text {seed}" for i in range(6)]
                for text in to_b: t.send(a, text)
                for text in to_a: t.send(b, text)
                self.assertEqual([t.receive(b) for _ in to_b], to_b)
                self.assertEqual([t.receive(a) for _ in to_a], to_a)
                self.assertTrue(a.flush() and b.flush())


class CloseTests(unittest.TestCase):

    def started(self, packets):
        r = make_rdt(packets)
        r.network.closed = True
        r.start()
        self.addCleanup(r.disconnect)
        return r

    def test_receive_stops_when_peer_closes(self):
        start = time.time()
        self.assertIsNone(self.started([]).receive())
        self.assertLess(time.time() - start, 0.5)

    def test_data_that_arrived_before_the_close_is_delivered(self):
        r = self.started([data(0, "a"), data(1, "b"), fin(2)])
        self.assertEqual(r.receive(), ["a", "b"])
        self.assertIsNone(r.receive())

    def test_flush_stops_when_peer_closes(self):
        r = self.started([])   # nothing is ever ACKed
        r.send(["a", "b"])
        start = time.time()
        self.assertFalse(r.flush())
        self.assertLess(time.time() - start, 0.5)

    def test_error_in_protocol_thread_reaches_the_application(self):
        r = make_rdt([])
        def fail(): raise RuntimeError("broken")
        r.network.udt_receive = fail
        r.start()
        with self.assertRaisesRegex(RuntimeError, "broken"):
            r.receive()


class FormatTimeTests(unittest.TestCase):

    def test_microseconds_are_zero_padded(self):
        self.assertTrue(log.format_time(1700000000 * 10**9 + 42000).endswith(".000042"))

    def test_uses_local_time(self):
        expected = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(1700000000))
        self.assertEqual(log.format_time(1700000000 * 10**9), expected + ".000000")


class NetworkTests(unittest.TestCase):

    def setUp(self):
        self.listener = socket.socket()
        self.listener.bind(("localhost", 0))
        self.listener.listen(1)
        with contextlib.redirect_stdout(io.StringIO()):
            self.net = NetworkLayer("client", "localhost", self.listener.getsockname()[1], 0, 0, 0)
        self.peer, _ = self.listener.accept()

    def tearDown(self):
        self.net.disconnect()
        self.peer.close()
        self.listener.close()

    def wait_closed(self):
        limit = time.time() + 2
        while not self.net.closed and time.time() < limit:
            time.sleep(0.01)
        return self.net.closed

    def read(self, length):
        out = ""
        limit = time.time() + 2
        while len(out) < length and time.time() < limit:
            out += self.net.udt_receive()
            time.sleep(0.01)
        return out

    def test_character_split_across_two_reads(self):
        raw = "é".encode("utf-8")
        self.peer.sendall(raw[:1])
        time.sleep(0.2)
        self.peer.sendall(raw[1:])
        self.assertEqual(self.read(1), "é")

    def test_invalid_bytes_do_not_stop_the_collector(self):
        self.peer.sendall(b"\xff")
        time.sleep(0.2)
        self.peer.sendall(b"ok")
        self.assertTrue(self.read(3).endswith("ok"))

    def test_partial_sends_keep_every_byte(self):
        class OneByteSocket:
            sent = b""
            def send(self, raw):
                self.sent += raw[:1]
                return 1
        conn, self.net.conn = self.net.conn, OneByteSocket()
        msg = data(0, "ação")
        self.net.udt_send(msg)
        fake, self.net.conn = self.net.conn, conn
        self.assertEqual(fake.sent.decode("utf-8"), msg)

    def test_full_socket_waits_for_the_peer_to_read(self):
        big = "x" * 5000000   # more than the socket buffers hold
        total = []
        def slow_reader():
            time.sleep(0.5)   # several socket timeouts
            while sum(total) < len(big):
                total.append(len(self.peer.recv(65536)))
        reader = threading.Thread(target=slow_reader, daemon=True)
        reader.start()
        self.net.udt_send(big)
        reader.join(5)
        self.assertEqual(sum(total), len(big))

    def test_disconnect_closes_the_connection(self):
        self.net.disconnect()
        self.peer.settimeout(1)
        self.assertEqual(self.peer.recv(10), b"")

    def test_peer_close_is_detected_after_its_last_data(self):
        self.peer.sendall(b"last")
        self.peer.close()
        self.assertTrue(self.wait_closed())
        self.assertEqual(self.net.udt_receive(), "last")
        self.net.collect_thread.join(1)
        self.assertFalse(self.net.collect_thread.is_alive())

    def test_abrupt_close_is_detected_and_sending_does_not_raise(self):
        # linger 0: close sends a reset instead of a normal close
        self.peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("hh" if os.name == "nt" else "ii", 1, 0))
        self.peer.close()
        self.assertTrue(self.wait_closed())
        for _ in range(3):
            self.net.udt_send(data(0, "a"))


class OptionsTests(unittest.TestCase):

    def parse(self, *args):
        parser = argparse.ArgumentParser()
        add_options(parser)
        with contextlib.redirect_stderr(io.StringIO()):
            return parser.parse_args(args)

    def test_values(self):
        args = self.parse("--window", "8", "--timeout", "0.5", "--loss", "0.2")
        self.assertEqual((args.window, args.timeout, args.loss, args.corrupt), (8, 0.5, 0.2, None))

    def test_invalid_values_are_rejected(self):
        for bad in (("--window", "0"), ("--window", "x"), ("--timeout", "0"), ("--timeout", "inf"),
                    ("--timeout", "nan"), ("--loss", "1.5"), ("--reorder", "-0.1")):
            with self.subTest(bad=bad), self.assertRaises(SystemExit):
                self.parse(*bad)


class MetricsTests(unittest.TestCase):

    def test_matplotlib_is_only_needed_for_plots(self):
        here = os.path.dirname(os.path.abspath(__file__))
        code = "import sys, metrics; sys.exit('matplotlib' in sys.modules)"
        self.assertEqual(subprocess.run([sys.executable, "-c", code], cwd=here).returncode, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
