import argparse


def probability(text):
    value = float(text)
    if not 0 <= value <= 1:
        raise argparse.ArgumentTypeError(f"{text} is not between 0 and 1")
    return value


def positive_int(text):
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"{text} is not 1 or more")
    return value


def positive_seconds(text):
    value = float(text)
    if not 0 < value < float('inf'):
        raise argparse.ArgumentTypeError(f"{text} is not a positive number of seconds")
    return value


# error rates of the simulated network
def add_error_options(parser):
    parser.add_argument('--loss', type=probability, default=None, help='Packet loss probability (default: 0.1).')
    parser.add_argument('--corrupt', type=probability, default=None, help='Packet corruption probability (default: 0.1).')
    parser.add_argument('--reorder', type=probability, default=None, help='Packet reorder probability (default: 0.1).')


# settings of the protocol
def add_protocol_options(parser):
    parser.add_argument('--window', type=positive_int, default=4, help='Window length in packets, the same on both sides (default: 4).')
    parser.add_argument('--timeout', type=positive_seconds, default=1, help='Seconds without an ACK before a packet is resent (default: 1).')


# every option shared by client and server
def add_options(parser):
    parser.add_argument('-q', '--quiet', help='Do not print the log of every packet.', action='store_true')
    add_protocol_options(parser)
    add_error_options(parser)
