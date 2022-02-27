#! /bin/bash

INSTALL_DIR=$HOME/.local

export LIBRARY_PATH=/usr/lib:/usr/local/lib:$INSTALL_DIR/lib:$INSTALL_DIR/lib/x86_64-linux-gnu
export LD_LIBRARY_PATH=/usr/lib:/usr/local/lib:$INSTALL_DIR/lib:$INSTALL_DIR/lib/x86_64-linux-gnu
export PATH=${PATH}:${INSTALL_DIR}/bin

SCRIPT=$1
shift 1

${SCRIPT} "$@"
