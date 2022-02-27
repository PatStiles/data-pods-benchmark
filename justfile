INSTALL_PREFIX := env_var("HOME") + "/.local"
BUILDTYPE := "release"
USE_TLS := "1"
USE_TVM := "0"
SGX_DEBUG := "0"
ENABLE_PROFILING := "0" # needs google-perftools libgoogle-perftools-dev
OFFLINE_MODE := "0"

MAX_ENCLAVE_THREADS := "42"

HEX_4GB := "0x100000000"
HEX_256KB := "0x40000"

MAX_ENCLAVE_STACK_SIZE := HEX_256KB
MAX_ENCLAVE_HEAP_SIZE := HEX_4GB

HOST_TARGET := if os() == "linux" {
    "x86_64-unknown-linux-gnu"
} else if os() == "macos" {
    "x86_64-apple-darwin"
} else if os() == "windows" {
    "x86_64-pc-windows-msvc"
} else {
    `exit 1 #unsupported OS`
}

SGX_TARGET := "x86_64-fortanix-unknown-sgx"

SGX_DEBUG_FLAG := if SGX_DEBUG == "1" { "-d" } else { "" }

BUILD_TYPE_FLAGS := if BUILDTYPE == "debug" { "--profile dev" } else { "--profile release" }
BUILDFLAGS := if OFFLINE_MODE == "1" { BUILD_TYPE_FLAGS+" --frozen" } else { BUILD_TYPE_FLAGS }

CLIENT_FEATURES := if USE_TLS == "1" { "--features=use-tls" } else { "" }

PROXY_FEATURES := if ENABLE_PROFILING == "1" { "--features=enable-profiling" } else { "--no-default-features" }
DATA_POD_UNSAFE_FEATURES := if ENABLE_PROFILING == "1" { "--features=enable-ctrlc,enable-profiling" } else { "--features=enable-ctrlc" }

DATA_POD_TVM_FEATURES := if USE_TVM == "1" { "--features=enable-tvm" } else { "" }
DATA_POD_FEATURES := if USE_TLS == "1" { DATA_POD_TVM_FEATURES+" --features=use-tls" } else { DATA_POD_TVM_FEATURES }

build: build-nosgx data-pod

build-nosgx: ledger data-pod-proxy data-pod-unsafe

data-pod-proxy:
    cargo build --package=data-pod-proxy --target={{HOST_TARGET}} {{BUILDFLAGS}}

data-pod-unsafe:
    cargo build --package=data-pod --target={{HOST_TARGET}} --no-default-features {{DATA_POD_FEATURES}} {{DATA_POD_UNSAFE_FEATURES}} {{BUILDFLAGS}}

ledger:
    cargo build --package=data-pods-ledger --target={{HOST_TARGET}} {{BUILDFLAGS}}

data-pod:
    cargo build --package=data-pod --target={{SGX_TARGET}} --no-default-features {{DATA_POD_FEATURES}} {{BUILDFLAGS}}

install-data-pod: data-pod
    ftxsgx-elf2sgxs target/{{SGX_TARGET}}/{{BUILDTYPE}}/data-pod --heap-size {{MAX_ENCLAVE_HEAP_SIZE}}?g --stack-size {{MAX_ENCLAVE_STACK_SIZE}} --threads {{MAX_ENCLAVE_THREADS}} {{SGX_DEBUG_FLAG}}
    sgxs-sign --key ./data-pod/enclave_key.pem target/{{SGX_TARGET}}/{{BUILDTYPE}}/data-pod.sgxs target/{{SGX_TARGET}}/{{BUILDTYPE}}/data-pod.sig --xfrm 7/0 --isvprodid 0 --isvsvn 0
    cp target/{{SGX_TARGET}}/{{BUILDTYPE}}/data-pod.sgxs {{INSTALL_PREFIX}}/bin/

install-data-pod-unsafe: data-pod-unsafe
    mkdir -p {{INSTALL_PREFIX}}/bin
    cp target/{{HOST_TARGET}}/{{BUILDTYPE}}/data-pod {{INSTALL_PREFIX}}/bin/data-pod-unsafe

install-ledger: ledger
    cargo install --path=ledger --force --target={{HOST_TARGET}}

install-data-pod-proxy: data-pod-proxy
    cargo install --path=data-pod-proxy --force --target={{HOST_TARGET}}

install-client-bindings:
    maturin develop -m client-bindings/Cargo.toml --cargo-extra-args="--no-default-features {{CLIENT_FEATURES}} {{BUILDFLAGS}}"

install: install-nosgx install-data-pod

install-nosgx: install-ledger install-data-pod-proxy install-data-pod-unsafe install-client-bindings

test-datastore:
    cargo test --package=data-pods-store {{BUILDFLAGS}}

test-data-pod:
    cargo test --package=data-pod --target={{HOST_TARGET}} --no-default-features {{DATA_POD_FEATURES}} {{DATA_POD_UNSAFE_FEATURES}}

test-utils:
    cargo test --package=data-pods-utils {{BUILDFLAGS}}

test: test-datastore test-utils test-data-pod

update-dependencies:
    cargo update

lint:
    cargo clippy --package=data-pod {{DATA_POD_FEATURES}}
    cargo clippy --package=data-pods-client-bindings {{CLIENT_FEATURES}}
    cargo clippy --package=data-pods-ledger
    cargo clippy --package=data-pod-proxy

cloc:
    cloc . --exclude-dir target

clobber:
    rm -rf ./target/

clean:
    rm ./target/{{HOST_TARGET}}/{{BUILDTYPE}}/block-sim -f
    rm ./target/{{HOST_TARGET}}/{{BUILDTYPE}}/data-pod -f
    rm ./target/{{SGX_TARGET}}/{{BUILDTYPE}}/data-pod -f
    rm ./target/{{SGX_TARGET}}/{{BUILDTYPE}}/data-pod.sgxs -f
    rm ./target/{{SGX_TARGET}}/{{BUILDTYPE}}/data-pod.sig -f

stopall:
    echo "Stopping all data pod related processes"
    killall -q data-pods-ledger || true
    killall -q data-pod-unsafe || true
    killall -q data-pod || true
    killall -q data-pod-proxy || true
    killall -q ftxsgx-runner || true
