FROM golang:1.26 as builder

ADD * /src

WORKDIR /src

RUN CGO_ENABLED=0 go build -o mctcp

FROM ubuntu:latest

COPY --from=builder /src/mctcp /app/mctcp

ENTRYPOINT ["/app/mctcp"]