FROM        alpine:3.2
ADD         raft-redis /bin/
VOLUME      /data
EXPOSE      6389 12379
ADD         run.sh /bin/run.sh
ENTRYPOINT  ["/bin/run.sh"]