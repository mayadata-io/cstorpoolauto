# Build cstorpoolauto binary
FROM golang:1.12.7 as builder

WORKDIR /

# copy go modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# ensure vendoring is up-to-date by running make vendor 
# in your local setup
#
# we cache the vendored dependencies before building and
# copying source so that we don't need to re-download when
# source changes don't invalidate our downloaded layer
RUN GO111MODULE=on go mod download
RUN GO111MODULE=on go mod vendor

# copy build manifests
COPY Makefile Makefile

# copy source files
COPY cmd/ ./cmd/
COPY k8s/ ./k8s/
COPY types/ ./types/
COPY util/ ./util/
COPY controller/ ./controller/

# build cspauto binary
RUN make

# Use debian as minimal base image to package the final binary
FROM debian:stretch-slim

WORKDIR /

RUN apt-get update && \
  apt-get install --no-install-recommends -y ca-certificates && \
  rm -rf /var/lib/apt/lists/*

COPY config/metac.yaml /etc/config/metac/metac.yaml
COPY --from=builder /cstorpoolauto /usr/bin/

CMD ["/usr/bin/cstorpoolauto"]