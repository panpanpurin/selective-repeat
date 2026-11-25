import argparse
import os
import subprocess
import sys
import time

# name: (loss, corruption, reorder)
SCENARIOS = {
    "no_error":   (0.0, 0.0, 0.0),
    "loss":       (0.1, 0.0, 0.0),
    "corruption": (0.0, 0.1, 0.0),
    "reorder":    (0.0, 0.0, 0.1),
    "all":        (0.1, 0.1, 0.1),
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run every error scenario and save its plots in plots/<scenario>/.')
    parser.add_argument('-n', '--messages', help='Messages per scenario (default: 5).', type=int, default=5)
    parser.add_argument('--port', help='First port; each scenario uses the next one (default: 5000).', type=int, default=5000)
    parser.add_argument('--out', help='Output directory (default: plots).', default='plots')
    args = parser.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    runs = []

    # scenarios are independent, so run them all at once on different ports
    for i, (name, (loss, corrupt, reorder)) in enumerate(SCENARIOS.items()):
        port = str(args.port + i)
        out = os.path.join(args.out, name)
        os.makedirs(out, exist_ok=True)
        errors = ['--loss', str(loss), '--corrupt', str(corrupt), '--reorder', str(reorder)]

        server_log = open(os.path.join(out, 'server.log'), 'w')
        client_log = open(os.path.join(out, 'client.log'), 'w')

        server = subprocess.Popen([sys.executable, '-u', os.path.join(here, 'server.py'), port] + errors,
                                  stdout=server_log, stderr=subprocess.STDOUT)
        time.sleep(1)  # let the server start listening
        client = subprocess.Popen([sys.executable, '-u', os.path.join(here, 'client.py'), 'localhost', port,
                                   '-n', str(args.messages), '--plots', out] + errors,
                                  stdout=client_log, stderr=subprocess.STDOUT)
        runs.append((name, server, client, server_log, client_log))
        print(f"Started {name} on port {port}")

    for name, server, client, server_log, client_log in runs:
        client.wait()
        # the server stops by itself when the client disconnects
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.terminate()
            server.wait()
        server_log.close()
        client_log.close()
        print(f"Finished {name} (client exit code {client.returncode})")
