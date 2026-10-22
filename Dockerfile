FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/nt-sync ./cmd/nt-sync

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/nt-sync /nt-sync
# The local SQLite copy lives here; mount a volume to keep it across restarts.
ENV SQLITE_PATH=/data/nt.db
WORKDIR /data
ENTRYPOINT ["/nt-sync"]
