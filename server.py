import argparse
from rdt import RDT, Transport
from metrics import Metrics
from network import add_error_args
import time

def upperCase(message):
    capitalizedSentence = message.upper()
    return capitalizedSentence


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='UPPER CASE server.')
    parser.add_argument('port', help='Port.', type=int)
    add_error_args(parser)
    args = parser.parse_args()

    rdt = RDT('server', None, args.port, args.loss, args.corrupt, args.reorder)
    transport = Transport()

    last_received_time = time.time()
    timeout_limit = 30

    try:

        while(True):
            
            if (last_received_time + timeout_limit < time.time()):
                break

            metricsServerSender = Metrics("Server - Sender")
            metricsServerReceiver = Metrics("Server - Receiver")

            text = transport.receive(rdt, metricsServerReceiver)

            if (text):

                last_received_time = time.time()

                reply = upperCase(text)
                transport.send(rdt, reply, metricsServerSender)

                print("||||||||||||||||||||||||||||||||||||||||||||||||||")
                print(f'Server: Original Sentence: \n\t{text}')
                print(f'Server: Converted Sentence Received: \n\t{reply}')
                print("||||||||||||||||||||||||||||||||||||||||||||||||||\n")

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

    del rdt