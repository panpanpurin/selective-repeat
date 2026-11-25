# Selective Repeat 
Implementation of the rdt4.0 protocol. The primary enhancement is the incorporation of the selective repeat mechanism, allowing for the seamless flow of multiple packets between a client and a server.

## Project Details

### Objective

The main objective of this project is to work within a layered network architecture, implementing functions to optimize link utilization using pipelining techniques.

### Implemented Functionalities

- **rdt4.0 Protocol:** Extension/modification of the initial stop-and-wait code to support the transmission of multiple packets between the client and server.

- **Selective Repeat:** Implementation of the more advanced version of the protocol, introducing selective repeat mechanisms for sliding window operation.

- **Sequence numbers:** The window length is N = 4 by default and sequence numbers go from 0 to 4N - 1, instead of the minimum of 2N usually given for selective repeat. 2N is only enough if the network never reorders packets, and this one does: a late copy of packet `s` can be released right after packet `s + N`, when the receiver's window has already moved to `[s + N + 1, s + 2N]`. With 2N numbers, `s` and `s + 2N` are the same number, so the old copy would be taken as new data. The same can happen with a late ACK. With 4N, an old number never falls inside the window, because the simulated network holds back only one packet at a time.

- **Full duplex:** Data flows in both directions at the same time over one connection. Each endpoint runs a protocol thread that owns both a sending and a receiving window, each with its own sequence numbers. The client queues all its messages at once, and the server's replies arrive while later messages are still being sent.

- **FIN handshake:** The sender ends each message with a FIN packet, which is resent until it's ACKed like any data packet, so the receiver knows the message is complete right away instead of waiting for a timeout, and never takes an incomplete message as complete. Both endpoints stop when the other closes the connection, or when nothing arrives for 30 seconds (or 5 packet timeouts, if that is longer). Sequence numbers continue from one message to the next, so a late copy of an old packet is recognized as old and never taken as new data.

### Provided Statistics

At the end of a run, the client prints the following statistics for each of its two roles. They are added up across all messages. **Sender** covers the data the client sends; **Receiver** covers the replies it receives and the ACKs it sends back.

- **Throughput (Network Layer):** Measurement of the transfer rate, considering packet headers.

- **Goodput (Application Layer Throughput):** Actual transfer rate excluding headers and retransmissions (sender only).

- **Total Transmitted Packets:** The overall count of packets sent during the simulation.

- **Total Retransmissions:** The count of data packets retransmitted after a timeout (sender only; ACKs are never retransmitted).

- **Total Corrupted Packets:** The count of packets identified as corrupted. On the sender side these are ACKs; on the receiver side they are data packets and FINs. If the corruption hit the packet's type, a packet without data is counted as an ACK.

- **Simulation Time:** Elapsed time from the start of transmission to the last packet sent.

With `--plots <dir>`, the client also saves plots of these metrics over time as PNG files in that directory.

## Code Usage

The code is designed to allow the transmission of multiple messages between the client and server. The number of messages can be configured as a command-line argument for the client (`-n`, default 5).

Install the dependencies (matplotlib, only needed for `--plots` and `scenarios.py`):

```bash
pip install -r requirements.txt
```

Start the server, then the client in another terminal:

```bash
python server.py 5000
```
```bash
python client.py localhost 5000 -n 5
```

Both print a log of every packet; use `-q` to turn it off.

### Code structure

| File | Role |
|---|---|
| `client.py`, `server.py` | Applications |
| `transport.py` | Splits messages into fragments and joins them back |
| `rdt.py` | Selective repeat protocol: windows, timers, ACKs and FIN |
| `packet.py` | Packet format and checksum |
| `network.py` | Simulated network with loss, corruption and reordering, over TCP |
| `metrics.py` | Statistics and plots |
| `options.py` | Command-line options |
| `log.py` | Packet log and time format |
| `scenarios.py` | Runs every error scenario |
| `test_rdt.py` | Unit tests |

### Error rates

The simulated network drops, corrupts and reorders packets with a probability of 0.1 each by default. Both the server and the client accept `--loss`, `--corrupt` and `--reorder` to change this. Each side applies its own rates to the packets it sends, so pass the same values to both:

```bash
python server.py 5000 --loss 0.2 --corrupt 0 --reorder 0
```
```bash
python client.py localhost 5000 --loss 0.2 --corrupt 0 --reorder 0 --plots plots/loss20
```

### Window and timeout

`--window` sets the window length in packets (default 4; 1 is stop-and-wait) and `--timeout` sets how many seconds a packet waits for its ACK before it's resent (default 1). The window must be the same on both sides, since the sequence numbers depend on it (otherwise the run stops with an error, or after 30 seconds without progress):

```bash
python server.py 5000 --window 8 --timeout 0.5
```
```bash
python client.py localhost 5000 --window 8 --timeout 0.5
```

### Scenarios

`scenarios.py` runs five scenarios at the same time (no error, 10% loss, 10% corruption, 10% reorder, and all three at 10%). It saves each scenario's plots and logs in `plots/<scenario>/`:

```bash
python scenarios.py -n 5
```

It also accepts `--window` and `--timeout`. The scenarios use ports 5000 to 5004 (change the first one with `--port`), and a run takes about 1 to 2 minutes.

### Tests

The unit tests run the protocol against a fake network, so they need no server:

```bash
python -m unittest
```
---
