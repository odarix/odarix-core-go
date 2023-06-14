FROM golang:1.20.5
WORKDIR /go/src/github.com/odarix/odarix-core-go
COPY . .
ENV CGO_CFLAGS="-Wno-error"
ENV CGO_ENABLED="1"
ENV GOOS=linux
# CMD go test ./...
