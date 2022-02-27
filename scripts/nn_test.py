#! /usr/bin/env python3

import data_pods

from multiprocessing import Process
from subprocess import Popen
from os.path import expanduser
from time import sleep

import numpy as np
import tvm
from tvm import te
from tvm import relay
from tvm.relay import testing

import argparse

APP_NAME='recommender'
BATCH_SIZE = 10
INPUT_DIMENSIONS = 8

def create_model():
    dshape = (BATCH_SIZE, INPUT_DIMENSIONS)

    data = relay.var('data', shape=dshape)
    fc = relay.nn.dense(data, relay.var("dense_weight"), units=dshape[-1]*2)
    fc = relay.nn.bias_add(fc, relay.var("dense_bias"))
    left, right = relay.split(fc, indices_or_sections=2, axis=1)
    one = relay.const(1, dtype="float32")

    net = relay.Tuple([(left + one), (right - one), fc])
    return testing.create_workload(net)

def run_inference(identifier, server_address, model_id):
    c = data_pods.create_client(identifier, server_address, 'localhost')

    input_vec = [float(x) for x in range(0, INPUT_DIMENSIONS*BATCH_SIZE)]

    c.call(APP_NAME, 'infer', args=[model_id, input_vec])

    print("Client #" + str(identifier) + " done.")
    c.close()

def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--startup_delay", type=int, default=40, help="How long the enclave takes to start up")
    parser.add_argument("--profiler", type=str, default="", help="The profiling tool to use", choices=['', 'massif', 'callgrind', 'cachegrind'])
    parser.add_argument("--use_sgx", action="store_true", default=False)
    parser.add_argument("--num_clients", type=int, default=10, help="The number of clients to run")

    args = parser.parse_args()

    blockchain = None
    datapod = None
    proxy = None

    def shutdown(exitcode):
        if proxy:
            proxy.terminate()
            proxy.wait()

        if datapod:
            datapod.terminate()
            datapod.wait()

        if blockchain:
            blockchain.terminate()
            blockchain.wait()

        exit(exitcode)

    print("## Running data pod test for neural network data")
    
    print("# Starting blockchain simulator")
    blockchain = Popen(["data-pods-ledger", "--listen=localhost", "--epoch_length=10"])

    print("# Starting data pod")
    enclave_port = 18080 

    server_addr = "localhost:50000"
    server_name = "server0"

    if args.use_sgx:
        # FIXME it will always default to port 18080 for now and ignore the command line argument
        datapod = Popen(["ftxsgx-runner", expanduser("~/.local/bin/data-pod.sgxs")])
    else:
        if args.profiler == "":
            datapod = Popen([expanduser("~/.local/bin/data-pod-unsafe"), str(enclave_port)])
        elif args.profiler == "massif":
            datapod = Popen(["valgrind", "--tool="+args.profiler, expanduser("~/.local/bin/data-pod-unsafe"), str(enclave_port)])
        else:
            datapod = Popen(["cargo-profiler", args.profiler, "--bin", expanduser("~/.local/bin/data-pod-unsafe"), str(encalve_port)])

    sleep(args.startup_delay)

    proxy = Popen(["data-pod-proxy", "--enclave_port="+str(enclave_port), "--listen="+server_addr, "--server_name="+str(server_name)])

    sleep(2)

    c = data_pods.create_client(0, server_addr, 'localhost')
    c.create_application('recommender', 'applications/recommender.app')

    sleep(2)
 
    mod, params = create_model()
    graph, lib, params = relay.build(
        mod, 'llvm --system-lib', params=params)

    pbytes = tvm.relay.save_param_dict(params)

    mod_id = c.call(APP_NAME, 'create_model', args=[graph, bytes(pbytes)])
    c.close()

    print("# Created application and model")

    sleep(0.5)

    print("# Running inference")
    clients = []
    for cid in range(args.num_clients):
        p = Process(target=run_inference, args=(cid, server_addr, mod_id))
        p.start()
        clients.append(p)

    for client in clients:
        client.join()

    print("#DONE")
    shutdown(0)

if __name__ == "__main__": main()
