import os
import time
import matplotlib
matplotlib.use("Agg")  # plots are saved to files, no window needed
import matplotlib.pyplot as plt
from rdt import Packet, format_time

HEADER_LEN = Packet.size_len + Packet.seq_num_len + Packet.checksum_len + 1

class Metrics:
    def __init__(self, label=""):
        self.label = label

        self.start_time = None
        self.end_time = None

        # (timestamp, packet) for every packet sent
        self.packets_sent = []

        # (timestamp, packet) for packets sent for the first time
        self.packets_first_sent = []

        # (timestamp, packet) for retransmissions
        self.retransmissions_sent = []

        # timestamps of corrupted packets received
        self.corrupted_received = []


    # only the first call counts
    def set_start(self):
        if (self.start_time == None):
            self.start_time = time.time_ns()

    # every call overrides the previous one
    def set_end(self):
        self.end_time = time.time_ns()

    def add_packet_sent(self, p):
        t = time.time_ns()
        self.packets_sent.append((t, p))

    def add_packet_first_sent(self, p):
        t = time.time_ns()
        self.packets_first_sent.append((t, p))

    def add_retransmission_sent(self, p):
        t = time.time_ns()
        self.retransmissions_sent.append((t, p))

    def add_corrupted_received(self):
        t = time.time_ns()
        self.corrupted_received.append(t)

    def elapsed(self):
        if self.start_time is None or self.end_time is None:
            return 0
        return (self.end_time - self.start_time) / 1000000000

    def throughput(self):
        total = sum(len(p.packet) for _, p in self.packets_sent)
        return total / self.elapsed() if self.elapsed() else 0

    def goodput(self):
        total = sum(len(p.packet) - HEADER_LEN for _, p in self.packets_first_sent)
        return total / self.elapsed() if self.elapsed() else 0

    def plot_simulation_time(self):
        if self.start_time is None:
            print(f"For {self.label}: nothing was sent\n")
            return
        print(f"For {self.label}")
        print("Start time: " + format_time(self.start_time) )
        print("End time: " + format_time(self.end_time ) )
        print(f"Difference: {self.elapsed()} (s) \n")

    # data=False skips retransmissions and goodput, which are always zero on the receiving side
    def print_summary(self, data=True):
        print(f"Summary for {self.label}")
        print(f"\tSimulation time: {self.elapsed():.3f} s")
        print(f"\tTransmitted packets: {len(self.packets_sent)}")
        if data:
            print(f"\tRetransmissions: {len(self.retransmissions_sent)}")
        print(f"\tCorrupted packets received: {len(self.corrupted_received)}")
        print(f"\tThroughput: {self.throughput():.2f} B/s")
        if data:
            print(f"\tGoodput: {self.goodput():.2f} B/s")
        print()

    # x = seconds since start, y = running total of `weights`
    def _plot_cumulative(self, path, title, ylabel, times, weights=None, isOrange=0):
        plt.figure(figsize=(12, 6))
        plt.title(title)
        plt.xlabel("Time (s)")
        plt.ylabel(ylabel)

        start = self.start_time or 0
        x = []
        y = []
        acc = 0
        for i, t in enumerate(times):
            acc += weights[i] if weights else 1
            x.append((t - start) / 1000000000)
            y.append(acc)

        plt.plot(x, y, marker="o", color=("tab:orange" if isOrange else "tab:blue"))
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(path)
        plt.close()

    def plot_corrupted(self, path, isOrange=0):
        self._plot_cumulative(path, f"Corrupted Packets in {self.label}: {len(self.corrupted_received)}",
                              "Corrupted Packets (unit)", self.corrupted_received, isOrange=isOrange)

    def plot_retransmissions(self, path, isOrange=0):
        self._plot_cumulative(path, f"Retransmitted Packets in {self.label}: {len(self.retransmissions_sent)}",
                              "Retransmitted Packets (unit)", [t for t, _ in self.retransmissions_sent], isOrange=isOrange)

    def plot_sentPacket(self, path, isOrange=0):
        self._plot_cumulative(path, f"All Transmitted Packets in {self.label}: {len(self.packets_sent)}",
                              "Transmitted Packets (unit)", [t for t, _ in self.packets_sent], isOrange=isOrange)

    def plot_throughput(self, path, isOrange=0):
        self._plot_cumulative(path, f"Throughput in {self.label}: {self.throughput():.2f} (B/s)",
                              "Transmitted Data (Bytes)", [t for t, _ in self.packets_sent],
                              [len(p.packet) for _, p in self.packets_sent], isOrange=isOrange)

    def plot_goodput(self, path, isOrange=0):
        self._plot_cumulative(path, f"Goodput in {self.label}: {self.goodput():.2f} (B/s)",
                              "Transmitted Data (Bytes)", [t for t, _ in self.packets_first_sent],
                              [len(p.packet) - HEADER_LEN for _, p in self.packets_first_sent], isOrange=isOrange)

    # save every plot into `directory`, file names prefixed with `prefix`
    # data=False skips retransmissions and goodput, which are always empty on the receiving side
    def save_plots(self, directory, prefix, data=True, isOrange=0):
        os.makedirs(directory, exist_ok=True)
        path = lambda name: os.path.join(directory, f"{prefix}_{name}.png")

        self.plot_sentPacket(path("sent"), isOrange)
        self.plot_throughput(path("throughput"), isOrange)
        self.plot_corrupted(path("corrupted"), isOrange)
        if data:
            self.plot_retransmissions(path("retransmissions"), isOrange)
            self.plot_goodput(path("goodput"), isOrange)
