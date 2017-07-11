FROM golang
ADD . /go/src/github.com/widaT/raft-redis
RUN go install github.com/widaT/raft-redis
EXPOSE 6389 6389
ENTRYPOINT ["raft-redis"]