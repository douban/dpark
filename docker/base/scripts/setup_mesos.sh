#!/bin/bash

cd /tmp
wget -q 'https://codeload.github.com/windreamer/mesos/tar.gz/master' -O -| tar xfz -
cd /tmp/mesos-master
./bootstrap
./configure --disable-java
make install
cd /tmp
rm -rf /tmp/mesos-master
