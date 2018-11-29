#!/bin/bash
set -xe

cmake3 -DUIUC_SHIBPLUGINS_INSTALL_DIR:PATH=/output /source
make
