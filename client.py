import argparse
from rdt import RDT, Transport
from metrics import Metrics

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Quotation client talking to an UPPER CASE server.')
    parser.add_argument('server', help='Server.')
    parser.add_argument('port', help='Port.', type=int)
    parser.add_argument('-n', '--messages', help='Number of messages to send (default: 5).', type=int, default=5)
    args = parser.parse_args()

    quotes = [
        'The art of debugging is figuring out what you really told your program to do rather than what you thought you told it to do. -- Andrew Singer', 
    	'The good news about computers is that they do what you tell them to do. The bad news is that they do what you tell them to do. -- Ted Nelson', 
    	'It is hardware that makes a machine fast. It is software that makes a fast machine slow. -- Craig Bruce',
        'The art of debugging is figuring out what you really told your program to do rather than what you thought you told it to do. -- Andrew Singer',
        'The computer was born to solve problems that did not exist before. - Bill Gates'
    ]

    # repeat the quotes if more messages than quotes are requested
    document = [quotes[i % len(quotes)] for i in range(args.messages)]

    rdt = RDT('client', args.server, args.port)
    transport = Transport()

    try:

        for text in document:

            metricsClientSender = Metrics("Client - Sender")
            metricsClientReceiver = Metrics("Client - Receiver")

            transport.send(rdt, text, metricsClientSender)
            reply = transport.receive(rdt, metricsClientReceiver)

            print("||||||||||||||||||||||||||||||||||||||||||||||||||")
            print(f'Client: Original Sentence: \n\t{text}')
            print(f'Client: Converted Sentence Received: \n\t{reply}')
            print("||||||||||||||||||||||||||||||||||||||||||||||||||\n")

        metricsClientSender.plot_simulation_time()
        metricsClientReceiver.plot_simulation_time()

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