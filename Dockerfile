FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -o /worker ./cmd/worker

FROM gcr.io/distroless/static-debian12 AS server
COPY --from=builder /server /server
ENTRYPOINT ["/server"]

FROM alpine:3 AS worker
COPY --from=builder /worker /worker
ENTRYPOINT ["/worker"]
