import argparse
import sys
import log
from rdt import RDT
from transport import Transport
from metrics import Metrics
from options import add_options

def upperCase(message):
    capitalizedSentence = message.upper()
    return capitalizedSentence


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='UPPER CASE server.')
    parser.add_argument('port', help='Port.', type=int)
    add_options(parser)
    args = parser.parse_args()

    log.debug = not args.quiet

    # accumulated across all messages
    metricsServerSender = Metrics("Server - Sender")
    metricsServerReceiver = Metrics("Server - Receiver")

    rdt = RDT('server', None, args.port, metricsServerSender, metricsServerReceiver,
              args.loss, args.corrupt, args.reorder, args.window, args.timeout)
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

            log.show("||||||||||||||||||||||||||||||||||||||||||||||||||\n"
                     f"Server: Original Sentence: \n\t{text}\n"
                     f"Server: Converted Sentence Sent: \n\t{reply}\n"
                     "||||||||||||||||||||||||||||||||||||||||||||||||||\n")

        rdt.disconnect()

        metricsServerReceiver.plot_simulation_time()
        metricsServerSender.plot_simulation_time()

    except KeyboardInterrupt:
        print("Ending connection...")

    except RuntimeError as err:
        sys.exit(f"Error: {err}")

    finally:
        rdt.disconnect()
        print("Connection ended.")
