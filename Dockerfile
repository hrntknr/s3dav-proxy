FROM golang:1.18-alpine AS builder

WORKDIR /app
COPY . .
RUN go build -o /app/s3dav-proxy .

FROM alpine:3.14

RUN apk add --no-cache ca-certificates
COPY --from=builder /app/s3dav-proxy /usr/local/bin/s3dav-proxy

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/s3dav-proxy"]
