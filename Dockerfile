FROM golang:1.26 as builder

ADD . /src

WORKDIR /src

ENV CGO_ENABLED=0

RUN go build -o udp2mctcp

FROM ubuntu:latest

COPY --from=builder /src/udp2mctcp /app/udp2mctcp

WORKDIR /app

ENTRYPOINT ["/app/udp2mctcp"]