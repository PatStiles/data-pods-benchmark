#!/bin/sh

#install nightly rust toolchain
curl  https://sh.rustup.rs -sSf | sh -s -- --default-toolchain nightly -y && source ~/.cargo/env

#install rust-sgx, just
rustup target add x86_64-fortanix-unknown-sgx --toolchain nightly
cargo install fortanix-sgx-tools sgxs-tools
cargo install just
echo >> ~/.cargo/config -e '[target.x86_64-fortanix-unknown-sgx]\nrunner = "ftxsgx-runner-cargo"'

#Install just using prebuilt binaries
# create `~/bin`
mkdir -p ~/bin

# download and extract `just` to `~/bin/just`
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to ~/bin

# add `~/bin` to the paths that your shell searches for executables
# this line should be added to your shells initialization file,
# e.g. `~/.bashrc` or `~/.zshrc`
setenv PATH ${PATH}:${HOME}/bin

#Install protobuf
sudo apt-get install autoconf automake libtool curl make g++ unzip

#Install libssl
sudo apt-get update -y
sudo apt-get install -y libssl-dev

#install 
#install Data-pods

just install-nosgx

cd scripts
python testnet.py
