from dpark/base:latest
MAINTAINER windreamer windreamer@gmail.com

RUN apt-get install -y dnsmasq && apt-get clean
ADD etc /etc
ADD mfs /usr/local/etc/mfs
ADD scripts /tmp/scripts
RUN cp /usr/local/var/mfs/metadata.mfs.empty /var/run/mfs/metadata.mfs
EXPOSE 22 5050 9420 9425
CMD ["/tmp/scripts/start.sh"]
