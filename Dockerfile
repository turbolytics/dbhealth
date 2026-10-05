# dbhealth: one static binary in a minimal image. Build with
#   docker build --build-arg VERSION=$(git describe --tags --always) .
ARG GO_IMAGE=golang:1.26-bookworm
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian12:nonroot

FROM ${GO_IMAGE} AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X github.com/turbolytics/dbhealth/internal/report.Version=${VERSION} -X github.com/turbolytics/dbhealth/internal/report.Commit=${COMMIT}" \
    -o /dbhealth ./cmd/dbhealth

FROM ${RUNTIME_IMAGE}
COPY --from=builder /dbhealth /usr/local/bin/dbhealth
ENTRYPOINT ["/usr/local/bin/dbhealth"]
CMD ["run", "-c", "/etc/dbhealth/dbhealth.yml"]
