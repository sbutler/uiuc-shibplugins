#!/bin/bash
set -xe

if [[ ! -e /source/.git ]]; then
    git clone -b $UIUC_SHIBPLUGINS_GITBRANCH $UIUC_SHIBPLUGINS_GITURL /source/
fi

cmake3 -DUIUC_SHIBPLUGINS_INSTALL_DIR:PATH=/output /source
make
