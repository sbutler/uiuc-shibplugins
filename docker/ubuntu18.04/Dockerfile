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

WORKDIR /build
CMD ["/bin/build.sh"]
