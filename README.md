# Data Pods
Research prototype for the FleetDB architecture.

[![Build Status](https://travis-ci.com/kaimast/data-pods.svg?token=8nNhnZqBJD8ys1A271z4&branch=master)](https://travis-ci.com/kaimast/data-pods)

## Installation
* a recent version of rust-nightly
* python3 and pip
* [rust-sgx](https://github.com/fortanix/rust-sgx)
* [just](https://github.com/casey/just)
* [maturin](https://github.com/PyO3/maturin)
* protobuf-compiler
* libssl-dev

## Compilation
Then, run the justfile from the root folder.
```
just install
```

### TVM Support

Data pods support machine learning applications through [TVM](https://github.com/apache/tvm).
To enable this supportyou need a recent version of TVM and its [Python modules](https://tvm.apache.org/docs/install/from_source.html#python-package-installation) installed. Additionally you need numpy.

Then you can run the following to install data pods with TVM support.
```
just USE_TVM=1 install
```

## Terminate data-pod execution
```
killall data-pod-unsafe data-pod-proxy data-pods-ledger
```

For machine learning to work you need to have the TVM python bindings installed.

## Debugging
You can enable logging by setting the `RUST_LOG` environment variable (see the [env_logger](https://docs.rs/env_logger/latest/env_logger/) documentation) and backtraces by setting the `RUST_BACKTRACE` environment variable.

Right now, data pods will only generate log information when running in unsecure mode.

