#! /usr/bin/env python3

import sys

from tvm import relay
from tvm.relay import testing

#FIXME build library for generic model here
DSHAPE = (10, 8)

def create_model():
    data = relay.var('data', shape=DSHAPE)
    fc = relay.nn.dense(data, relay.var("dense_weight"), units=DSHAPE[-1]*2)
    fc = relay.nn.bias_add(fc, relay.var("dense_bias"))
    left, right = relay.split(fc, indices_or_sections=2, axis=1)
    one = relay.const(1, dtype="float32")

    net = relay.Tuple([(left + one), (right - one), fc])
    return testing.create_workload(net)

def main():
    mod, params = create_model()
    _graph, lib, _params = relay.build(
        mod, 'llvm --system-lib', params=params)

    lib.save(sys.argv[1])

if __name__ == '__main__':
    main()
