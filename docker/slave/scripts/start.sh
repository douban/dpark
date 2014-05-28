#!/bin/sh

if [ -z "${MASTER_IP}" ]
then
    exit 1
fi
SLAVE_IP=$(ip addr | awk '/inet/ && /eth0/{sub(/\/.*$/,"",$2); print $2}')
mknod /dev/fuse c 10 229
sed -i "s/#MASTER#/${MASTER_IP}/g" /usr/local/etc/mfs/mfschunkserver.cfg
mfschunkserver -d &
MFS_CHUNKSERVER_PID=$!
mesos-slave --master=${MASTER_IP}:5050 --hostname=${SLAVE_IP} &
/usr/sbin/sshd
mfsmount /mfs -H ${MASTER_IP}
wait ${MFS_CHUNKSERVER_PID}
