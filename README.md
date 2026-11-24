# Selective Repeat 
Implementation of the rdt4.0 protocol. The primary enhancement is the incorporation of the selective repeat mechanism, allowing for the seamless flow of multiple packets between a client and a server.

## Project Details

### Objective

The main objective of this project is to work within a layered network architecture, implementing functions to optimize link utilization using pipelining techniques.

### Implemented Functionalities

- **rdt4.0 Protocol:** Extension/modification of the initial stop-and-wait code to support the transmission of multiple packets between the client and server.

- **Selective Repeat:** Implementation of the more advanced version of the protocol, introducing selective repeat mechanisms for sliding window operation.

### Provided Statistics

At the end of a run, the client prints the following statistics for each of its two roles. They are added up across all messages. **Sender** covers the data the client sends; **Receiver** covers the replies it receives and the ACKs it sends back.

- **Throughput (Network Layer):** Measurement of the transfer rate, considering packet headers.

- **Goodput (Application Layer Throughput):** Actual transfer rate excluding headers and retransmissions (sender only).

- **Total Transmitted Packets:** The overall count of packets sent during the simulation.

- **Total Retransmissions:** The count of data packets retransmitted after a timeout (sender only; ACKs are never retransmitted).

- **Total Corrupted Packets:** The count of packets identified as corrupted. On the sender side these are ACKs; on the receiver side they are data packets.

- **Simulation Time:** Elapsed time from the start of transmission to the last packet sent.

With `--plots <dir>`, the client also saves plots of these metrics over time as PNG files in that directory.

## Code Usage

The code is designed to allow the transmission of multiple messages between the client and server. The number of messages can be configured as a command-line argument for the client (`-n`, default 5).

Install the dependencies:

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

### Error rates

The simulated network drops, corrupts and reorders packets with a probability of 0.1 each by default. Both the server and the client accept `--loss`, `--corrupt` and `--reorder` to change this. Each side applies its own rates to the packets it sends, so pass the same values to both:

```bash
python server.py 5000 --loss 0.2 --corrupt 0 --reorder 0
```
```bash
python client.py localhost 5000 --loss 0.2 --corrupt 0 --reorder 0 --plots plots/loss20
```

### Scenarios

`scenarios.py` runs five scenarios at the same time (no error, 10% loss, 10% corruption, 10% reorder, and all three at 10%). It saves each scenario's plots and logs in `plots/<scenario>/`:

```bash
python scenarios.py -n 5
```

The scenarios use ports 5000 to 5004 (change the first one with `--port`), and a run takes about 2 to 3 minutes.
---
