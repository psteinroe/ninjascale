FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ninjascale ./cmd/ninjascale

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /ninjascale /usr/local/bin/ninjascale

ENTRYPOINT ["ninjascale"]
CMD ["-config", "/etc/ninjascale/config.yaml"]
