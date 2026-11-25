import argparse
from rdt import RDT, Transport
from metrics import Metrics
from network import add_error_args

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Quotation client talking to an UPPER CASE server.')
    parser.add_argument('server', help='Server.')
    parser.add_argument('port', help='Port.', type=int)
    parser.add_argument('-n', '--messages', help='Number of messages to send (default: 5).', type=int, default=5)
    parser.add_argument('--plots', help='Directory to save the metric plots in (default: no plots).', default=None)
    add_error_args(parser)
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

    rdt = RDT('client', args.server, args.port, args.loss, args.corrupt, args.reorder)
    transport = Transport()

    # accumulated across all messages
    metricsClientSender = Metrics("Client - Sender")
    metricsClientReceiver = Metrics("Client - Receiver")

    try:

        for text in document:

            transport.send(rdt, text, metricsClientSender)
            reply = transport.receive(rdt, metricsClientReceiver)

            if reply is None and rdt.closed():
                print("Client: the server closed the connection")
                break

            print("||||||||||||||||||||||||||||||||||||||||||||||||||")
            print(f'Client: Original Sentence: \n\t{text}')
            print(f'Client: Converted Sentence Received: \n\t{reply}')
            print("||||||||||||||||||||||||||||||||||||||||||||||||||\n")

        metricsClientSender.print_summary()
        metricsClientReceiver.print_summary(data=False)

        if args.plots:
            metricsClientSender.save_plots(args.plots, "sender")
            metricsClientReceiver.save_plots(args.plots, "receiver", data=False, isOrange=1)
            print(f"Plots saved in {args.plots}")

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