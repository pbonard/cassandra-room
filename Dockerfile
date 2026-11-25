# cassandra-room server image: the cassandra-room binary on a minimal base.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/cassandra-room ./cmd/cassandra-room

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/cassandra-room /usr/local/bin/cassandra-room
# CQL, internode, management API
EXPOSE 9042 7000 7199
VOLUME /var/lib/cassandra-room
ENTRYPOINT ["/usr/local/bin/cassandra-room", "-data-dir=/var/lib/cassandra-room"]
