#!/bin/sh
if [ -z $1 ]
then
    SLAVE_NUM=1
else
    SLAVE_NUM=$1
fi

echo $SLAVES

MASTER=$(docker run --privileged=true --dns 127.0.0.1 -d dpark/master)
MASTER_IP=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $MASTER)
SLAVES=()
for i in $(seq ${SLAVE_NUM})
do
    SLAVES+=($(docker run --privileged=true --dns ${MASTER_IP} -e MASTER_IP=${MASTER_IP} -d dpark/slave))
done

trap cleanup 2

cleanup()
{
    docker kill ${MASTER}
    for SLAVE in ${SLAVES}
    do
        docker kill ${SLAVE}
    done
    docker rm ${MASTER}
    for SLAVE in ${SLAVES}
    do
        docker rm ${SLAVE}
    done
    exit 0
}

sleep 3
echo password is dpark
ssh -l dpark ${MASTER_IP}

cleanup

