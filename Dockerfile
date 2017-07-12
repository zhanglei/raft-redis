FROM        alpine:3.2
ADD         raft-redis /bin/
VOLUME      /data
EXPOSE      2379 2380
ADD         run.sh /bin/run.sh
ENTRYPOINT  ["/bin/run.sh"]