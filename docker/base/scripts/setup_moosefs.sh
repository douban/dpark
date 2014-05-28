#!/bin/bash

cd /tmp
wget -q 'https://codeload.github.com/windreamer/moosefs/tar.gz/master' -O -| tar xfz -
cd /tmp/moosefs-master
./autogen.sh
./configure
make install
cd /tmp
rm -rf /tmp/moosefs-master
