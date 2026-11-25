import argparse
import log
from rdt import RDT
from transport import Transport
from metrics import Metrics
from network import add_error_args

def upperCase(message):
    capitalizedSentence = message.upper()
    return capitalizedSentence


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='UPPER CASE server.')
    parser.add_argument('port', help='Port.', type=int)
    parser.add_argument('-q', '--quiet', help='Do not print the log of every packet.', action='store_true')
    add_error_args(parser)
    args = parser.parse_args()

    log.debug = not args.quiet

    # accumulated across all messages
    metricsServerSender = Metrics("Server - Sender")
    metricsServerReceiver = Metrics("Server - Receiver")

    rdt = RDT('server', None, args.port, metricsServerSender, metricsServerReceiver, args.loss, args.corrupt, args.reorder)
    transport = Transport()

    try:

        while(True):
            
            text = transport.receive(rdt)

            # the client disconnected or went silent
            if (text is None):
                break

            # queued and sent while the next messages are still arriving
            reply = upperCase(text)
            transport.send(rdt, reply)

            print("||||||||||||||||||||||||||||||||||||||||||||||||||")
            print(f'Server: Original Sentence: \n\t{text}')
            print(f'Server: Converted Sentence Sent: \n\t{reply}')
            print("||||||||||||||||||||||||||||||||||||||||||||||||||\n")

        rdt.disconnect()

        metricsServerReceiver.plot_simulation_time()
        metricsServerSender.plot_simulation_time()

    except (KeyboardInterrupt, SystemExit):
        rdt.disconnect()
        print("Ending connection...")

    except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
        rdt.disconnect()
        print("Ending connection...")
        
    finally:
        rdt.disconnect()
        print("Connection ended.")
