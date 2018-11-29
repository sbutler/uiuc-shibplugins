#!/bin/bash
set -xe

cmake -DUIUC_SHIBPLUGINS_INSTALL_DIR:PATH=/output /source
make
