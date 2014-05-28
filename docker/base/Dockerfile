from phusion/baseimage:latest
MAINTAINER windreamer windreamer@gmail.com

RUN export DEBCONF_FRONTEND='noninteractive'
ADD scripts /tmp/scripts
RUN sed -i 's/archive.ubuntu.com/mirrors.163.com/g' /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y build-essential autoconf automake libtool pkg-config wget git-core zlib1g-dev libfuse-dev libcurl4-openssl-dev libsasl2-dev python-dev libzmq-dev python-pip && apt-get clean
RUN sh /tmp/scripts/setup_moosefs.sh
RUN sh /tmp/scripts/setup_mesos.sh
RUN pip install -i http://pypi.douban.com/simple -r /tmp/scripts/requirements.txt
RUN mkdir /mfs
RUN useradd -r moosefs
RUN mkdir -p /var/run/mfs
RUN chown moosefs.moosefs /var/run/mfs
RUN useradd -m -u 1024 -s /bin/bash -G sudo dpark
RUN echo "dpark:dpark" | chpasswd
RUN ldconfig
RUN ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key -q -N ""
