FROM centos:7

VOLUME /source
VOLUME /output

RUN set -xe \
    && yum -y install epel-release \
    && yum -y groupinstall development \
    && yum -y install \
        cmake3 \
        libcurl-devel

COPY shibboleth.gpg /tmp/shibboleth.gpg
COPY shibboleth.repo /etc/yum.repos.d/
RUN set -xe \
    && rpm --import /tmp/shibboleth.gpg && rm /tmp/shibboleth.gpg \
    && yum -y install \
        shibboleth \
        shibboleth-devel

COPY build.sh /bin/
RUN chmod a+rx /bin/build.sh

WORKDIR /build
CMD ["/bin/build.sh"]
