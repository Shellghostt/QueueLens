FROM golang:1.23-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/queuelens ./cmd/queuelens

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/queuelens /usr/local/bin/queuelens
ENTRYPOINT ["/usr/local/bin/queuelens"]
