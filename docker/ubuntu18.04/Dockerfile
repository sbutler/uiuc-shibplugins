# Copyright (c) 2018 University of Illinois Board of Trustees
# All rights reserved.
#
# Developed by:       Technology Services
#                     University of Illinois at Urbana-Champaign
#                     https://techservices.illinois.edu/
#
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal with the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# - Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimers.
# - Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimers in the
#   documentation and/or other materials provided with the distribution.
# - Neither the names of Technology Services, University of Illinois at
#   Urbana-Champaign, nor the names of its contributors may be used to
#   endorse or promote products derived from this Software without
#   specific prior written permission.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR
# ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
# CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE SOFTWARE.
FROM ubuntu:18.04

VOLUME /source
VOLUME /output

RUN apt-get update
RUN apt-get -y install \
    build-essential \
    cmake \
    git \
    gnupg \
    libboost-all-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    uuid-dev \
    zlib1g-dev

COPY SWITCHaai-swdistrib.asc /root
COPY SWITCHaai-swdistrib.list /etc/apt/sources.list.d
RUN set -xe \
    && apt-key add /root/SWITCHaai-swdistrib.asc \
    && apt-get update
RUN apt-get -y install shibboleth libshibsp-dev

COPY build.sh /bin/
RUN chmod a+rx /bin/build.sh

ENV UIUC_SHIBPLUGINS_GITURL="https://github.com/sbutler/uiuc-shibplugins.git" \
    UIUC_SHIBPLUGINS_GITBRANCH="master"

WORKDIR /build
CMD ["/bin/build.sh"]
