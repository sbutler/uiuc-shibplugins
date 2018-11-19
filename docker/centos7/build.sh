#!/bin/bash
set -xe

cmake3 -DCMAKE_INSTALL_PREFIX:PATH=/output /source
make
make install
