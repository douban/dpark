#!/bin/sh

mknod /dev/fuse c 10 229
MASTER_IP=$(ip addr | awk '/inet/ && /eth0/{sub(/\/.*$/,"",$2); print $2}')
dnsmasq &
mfsmaster -d &
MFS_MASTER_PID=$!
mfscgiserv
mesos-master --cluster=mesos &
/usr/sbin/sshd
sleep 3
mfsmount /mfs -H ${MASTER_IP}
wait ${MFS_MASTER_PID}
