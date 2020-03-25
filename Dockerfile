# --------------------------
# Test cstorpoolauto binary
# --------------------------
FROM golang:1.13.5 as tester

WORKDIR /mayadata.io/cstorpoolauto/

# copy go modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# ensure vendoring is up-to-date by running make vendor 
# in your local setup
#
# we cache the vendored dependencies before building and
# copying source so that we don't need to re-download when
# source changes don't invalidate our downloaded layer
RUN go mod download
RUN go mod tidy
RUN go mod vendor

# copy build manifests
COPY Makefile Makefile

# copy source files
COPY cmd/ cmd/
COPY unstruct/ unstruct/
COPY types/ types/
COPY util/ util/
COPY controller/ controller/

# test cstorpoolauto
RUN make test

# --------------------------
# Build cstorpoolauto binary
# --------------------------
FROM golang:1.13.5 as builder

WORKDIR /mayadata.io/cstorpoolauto/

# copy go modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# ensure vendoring is up-to-date by running make vendor 
# in your local setup
#
# we cache the vendored dependencies before building and
# copying source so that we don't need to re-download when
# source changes don't invalidate our downloaded layer
RUN go mod download
RUN go mod tidy

# copy build manifests
COPY Makefile Makefile

# copy source files
COPY cmd/ cmd/
COPY unstruct/ unstruct/
COPY types/ types/
COPY util/ util/
COPY controller/ controller/

# build cstorpoolauto binary
RUN make cstorpoolauto

# ---------------------------
# Use distroless as minimal base image to package the final binary
# Refer https://github.com/GoogleContainerTools/distroless
# ---------------------------
FROM gcr.io/distroless/static:nonroot

WORKDIR /

COPY config/metac.yaml /etc/config/metac/metac.yaml
COPY config/localdevice/metac.yaml /etc/config/metac/localdevice/metac.yaml
COPY --from=builder /mayadata.io/cstorpoolauto/cstorpoolauto /usr/bin/

USER nonroot:nonroot

CMD ["/usr/bin/cstorpoolauto"]