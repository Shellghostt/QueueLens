FROM node:22.22.3-alpine AS frontend
WORKDIR /web
COPY web/package.json web/package-lock.json ./
COPY web/scripts/isolate-go-dependencies.mjs ./scripts/isolate-go-dependencies.mjs
RUN npm ci
COPY web/ ./
RUN npm run build && npm run check:assets

FROM golang:1.26.11-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=frontend /web/dist ./web/dist
RUN CGO_ENABLED=0 go build -tags dashboard -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/queuelens ./cmd/queuelens

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/queuelens /usr/local/bin/queuelens
ENTRYPOINT ["/usr/local/bin/queuelens"]
CMD ["serve"]
