#! /usr/bin/env python3

# pylint: disable=c-extension-no-member,too-many-statements,consider-using-with,too-many-locals,too-many-arguments,too-many-branches

from subprocess import Popen
from time import sleep
from os.path import expanduser
from multiprocessing import Process, SimpleQueue
from random import randint

import argparse
import sys

import data_pods

### Application Specific code ###
class Messenger:
    @staticmethod
    def name():
        return "messenger"

    def run_setup(self, cid, server_address, account_queue):
        client = data_pods.create_client(cid, server_address, 'localhost')

        name = f"client_{cid}"
        aid = client.call(self.name(), "create_account", args=[name])

        account_queue.put((cid, aid))

        aid_str = ''.join(format(x, '02x') for x in aid)
        print(f"Client #{cid} @{server_address} set up. Account id is {aid_str}")

        client.close()

    def check_result(self, args, server_addrs, accounts):
        print("# Counting messages")
        count_queue = SimpleQueue()
        clients = []
        result = 0

        for cid in range(args.num_clients):
            sid = cid % args.num_servers
            server_addr = server_addrs[sid]
            proc = Process(target=self._count_messages, args=(cid, server_addr, count_queue, accounts))

            proc.start()
            clients.append(proc)

        for client in clients:
            client.join()

            if client.exitcode != 0:
                result = client.exitcode

        if result != 0:
            print("ERROR: Counting failed.")
            return False

        message_count = 0

        while not count_queue.empty():
            num = count_queue.get()
            message_count += num

        # Messages show up twice (once for sender; once for receiver)
        expected = 2 * args.num_clients * args.num_batches * args.batch_size

        if message_count == expected:
            print("Message count is correct.")
            return True

        print(f"ERROR: Message count was {message_count}, but expected {expected}")
        return False

    def _count_messages(self, cid, server_address, count_queue, accounts):
        aid = accounts[cid]
        client = data_pods.create_client(cid, server_address, 'localhost')

        num = client.call(self.name(), "count_messages", args=[aid])

        if not num:
            raise RuntimeError("Did not get a valid result")

        count_queue.put(num)
        client.close()

    def create_op(self, client, identifier, num_clients, accounts):
        tid = identifier

        while tid == identifier:
            tid = randint(0, num_clients-1)

        src_name = accounts[identifier]
        dst_name = accounts[tid]

        msg = "oh hai"

        return client.call_async(self.name(), "send_message", args=[src_name, dst_name, msg])

class Cryptobirds:
    @staticmethod
    def name():
        return "cryptobirds"

    def run_setup(self, cid, server_address, account_queue):
        client = data_pods.create_client(cid, server_address, 'localhost')

        aid = client.call(self.name(), "create_random_bird", args=[])
        account_queue.put((cid, aid))

        aid_str = ''.join(format(x, '02x') for x in aid)
        print(f"Client #{cid} @{server_address} set up. Account id is {aid_str}")

        client.close()

    def check_result(self, _args, _server_addrs, _accounts):
        # TODO
        return True

    def create_op(self, client, _identifier, num_clients, accounts):
        sid = tid = randint(0, num_clients-1)

        while sid == tid:
            tid = randint(0, num_clients-1)

        parent1_name = accounts[sid]
        parent2_name = accounts[tid]

        return client.call_async(self.name(), "breed", args=[parent1_name, parent2_name])

### Boilerplate code ###
def run_ops(app, identifier, server_address, num_clients, batch_size, num_batches, accounts):
    client = data_pods.create_client(identifier, server_address, 'localhost')

    for _ in range(num_batches):
        ops = []

        for _ in range(batch_size):
            client_op = app.create_op(client, identifier, num_clients, accounts)
            ops.append(client_op)

        for client_op in ops:
            client_op.wait()

    print(f"Client #{identifier} done.")
    client.close()

def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--num_clients", type=int, default=10, help="The number of clients to run")
    parser.add_argument("--profiler", type=str, default="", help="The profiling tool to use", choices=['', 'massif', 'callgrind', 'cachegrind'])
    parser.add_argument("--batch_size", type=int, default=100, help="The number of database ops per batch")
    parser.add_argument("--num_batches", type=int, default=5, help="The number of batches of database ops per client")
    parser.add_argument("--num_servers", type=int, default=10, help="The number of data pod instances")
    parser.add_argument("--epoch_length", type=int, default=1, help="The length of a global blockchain epoch")
    parser.add_argument("--use_sgx", action="store_true", default=False)
    parser.add_argument("--application", type=str, choices=["messenger", "cryptobirds"], default="messenger")

    args = parser.parse_args()

    blockchain = None
    clients = []
    datapods = []
    proxies = []

    def shutdown(exitcode):
        for proxy in proxies:
            proxy.terminate()
            proxy.wait()

        for datapod in datapods:
            datapod.terminate()
            datapod.wait()

        if blockchain:
            blockchain.terminate()
            blockchain.wait()

        sys.exit(exitcode)

    print(f'## Running data pod test network for application "{args.application}"')

    print("# Starting blockchain simulator")
    blockchain = Popen(["data-pods-ledger", "--listen=localhost", f"--epoch_length={args.epoch_length}"])

    if args.num_clients < 2:
        print("ERROR: Need at least two clients!")
        shutdown(-1)

    if args.num_servers < 1:
        print("ERROR: Need at least one server")
        shutdown(-1)

    if args.application == 'messenger':
        app = Messenger()
    elif args.application == 'cryptobirds':
        app = Cryptobirds()
    else:
        raise RuntimeError(f"No such app: {args.application}")

    print("# Starting data pods")
    for i in range(args.num_servers):
        port = 18080 + i
        datapod = None

        if args.use_sgx:
            # FIXME it will always default to port 18080 for now and ignore the command line argument
            datapod = Popen(["ftxsgx-runner", expanduser("~/.local/bin/data-pod.sgxs"), "--", str(port)])
        else:
            if args.profiler == "":
                datapod = Popen([expanduser("~/.local/bin/data-pod-unsafe"), str(port)])
            elif args.profiler == "massif":
                datapod = Popen(["valgrind", "--tool="+args.profiler, expanduser("~/.local/bin/data-pod-unsafe"), str(port)])
            else:
                datapod = Popen(["cargo-profiler", args.profiler, "--bin", expanduser("~/.local/bin/data-pod-unsafe"), str(port)])

        datapods.append(datapod)

    server_addrs = []

    print("# Starting data pod proxies")
    for sid in range(args.num_servers):
        enclave_port = 18080 + sid
        listen_addr = f"localhost:{50000 + sid}"
        name = "server" + str(sid)
        server_addrs.append(listen_addr)

        proxy = Popen(["data-pod-proxy", f"--enclave_port={enclave_port}", "--listen="+listen_addr, "--server_name="+str(name)])
        proxies.append(proxy)

    sleep(1)

    print("# Creating application")
    client = data_pods.create_client(0, server_addrs[0], 'localhost')
    client.create_application(args.application, f'./applications/{args.application}.app')
    client.close()

    if args.num_servers > 1:
        print("# Setting up the federated network")
        for sid in range(args.num_servers):
            server_address = server_addrs[sid]
            cid = sid
            client = data_pods.create_client(cid, server_address, 'localhost')

            for peer_id in range(0, sid):
                print(f"Connecting {sid}<->{peer_id}")
                client.peer_connect(server_addrs[peer_id])

            sleep(0.5) #FIXME
            client.close()

    # Wait for connections to be established and the application to be created
    # FIXME stop using sleeps
    sleep(5)

    print("# Running setup")
    clients = []
    account_queue = SimpleQueue()

    for cid in range(args.num_clients):
        sid = cid % args.num_servers
        server_addr = server_addrs[sid]

        proc = Process(target=app.run_setup, args=(cid, server_addr, account_queue))
        proc.start()
        clients.append(proc)

    result = 0
    for client in clients:
        client.join()

        if client.exitcode != 0:
            result = client.exitcode

    if result != 0:
        print("ERROR: Setup failed. Shutting down...")
        shutdown(-1)

    accounts = {}
    clients = []

    while len(accounts) < args.num_clients:
        cid, aid = account_queue.get()

        if cid in accounts:
            print("ERROR: got same cid twice")
            shutdown(-1)
        else:
            accounts[cid] = aid

        print(f"Got {len(accounts)} of {args.num_clients} accounts")

    print("# Running operations")
    for cid in range(args.num_clients):
        sid = cid % args.num_servers
        server_addr = server_addrs[sid]

        proc = Process(target=run_ops, args=(app, cid, server_addr, args.num_clients, args.batch_size, args.num_batches, accounts))
        proc.start()
        clients.append(proc)

    result = 0

    for client in clients:
        client.join()

        if client.exitcode != 0:
            result = client.exitcode

    if result != 0:
        print("ERROR: Ops failed. Shutting down...")
        shutdown(-1)

    sleep(2)

    print("# Counting commits")
    commit_count = 0
    abort_count = 0
    success = True

    for sid in range(args.num_servers):
        server_address = server_addrs[sid]
        cid = sid
        client = data_pods.create_client(cid, server_address, 'localhost')
        stats = client.get_statistics(0, client.get_current_blockchain_epoch())

        commit_count += stats.num_committed_txs()
        abort_count += stats.num_aborted_txs()

        client.close()

    sleep(2)

    expected = args.num_clients * (1 + args.num_batches * args.batch_size)

    if commit_count == expected:
        print("Commit count is correct")
    else:
        print(f"ERROR: Commit count was {commit_count}, but expected {expected}")
        shutdown(-1)

    if abort_count == 0:
        print("Abort count is correct")
    else:
        print(f"ERROR: Abort count was {abort_count}, but expected 0")
        success = False

    # FIXME add read-only transactions to speed this up
    if not app.check_result(args, server_addrs, accounts):
        success = False

    print("Test is done...")

    if success:
        shutdown(0)
    else:
        shutdown(-1)


if __name__ == "__main__":
    main()
