from dpark/base:latest
MAINTAINER windreamer windreamer@gmail.com

ADD mfs /usr/local/etc/mfs
ADD scripts /tmp/scripts
RUN mkdir /mfsdata
RUN chown moosefs.moosefs /mfsdata
EXPOSE 22 5051 5055
CMD ["/tmp/scripts/start.sh"]

