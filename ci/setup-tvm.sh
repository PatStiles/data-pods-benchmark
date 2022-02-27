#! /bin/bash

sudo add-apt-repository ppa:kai-mast/tvm -y
sudo apt-get update

sudo apt-get install libtvm -y

#FIXME provide binary packages for python
git clone https://github.com/apache/incubator-tvm.git
cd tvm
git submodule update --init --recursive
git checkout 63f84a11353791ac3f8916cdcf7c2c6e6d45c4fb

cd topi/python
python3 ./setup.py install --user
cd ../../

cd python
python3 ./setup.py install --user
cd ..
