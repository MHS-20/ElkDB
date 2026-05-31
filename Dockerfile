# Stage 1: builder
FROM golang:1.26-alpine AS builder
RUN apk add --no-cache make
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN make all

# Stage 2: runtime (non-root)
FROM alpine:3.20
RUN addgroup -S elkdb && adduser -S elkdb -G elkdb
WORKDIR /app

COPY --from=builder /src/elkdb        ./elkdb
COPY --from=builder /src/elkdb-server ./elkdb-server
RUN mkdir -p /data && chown elkdb:elkdb /data

USER elkdb
EXPOSE 5433

ENV DB=/data/elkdb.db \
  ADDR=:5433

ENTRYPOINT ["./elkdb-server"]
CMD ["-db", "/data/elkdb.db", "-addr", ":5433"]
