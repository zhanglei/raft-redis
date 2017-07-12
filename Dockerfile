FROM golang
ADD . /go/src/github.com/widaT/raft-redis
RUN go install github.com/widaT/raft-redis
EXPOSE 6389 12379
VOLUME /data
ENTRYPOINT ["raft-redis"]