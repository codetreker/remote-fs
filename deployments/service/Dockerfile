FROM node:24.19.0-bookworm-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df AS console
WORKDIR /build/internal/console
RUN npm install --prefix /build-tools --no-audit --no-fund pnpm@10.34.5
ENV PATH="/build-tools/node_modules/.bin:${PATH}"
COPY internal/console/ ./
RUN pnpm install --frozen-lockfile && pnpm build && pnpm check:dist && pnpm test

FROM golang:1.26.8-bookworm@sha256:dc9ad6c05acc7a88e5b71bde60a5fe3bd4b9f0db209011711b464107438a8107 AS build
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=console /build/internal/console/dist/ internal/console/dist/
RUN CGO_ENABLED=0 go build -trimpath -o /build/bin/remote-fs-server ./cmd/remote-fs-server

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 remote-fs \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin remote-fs \
    && install -d -m 0700 -o 10001 -g 10001 /var/lib/remote-fs
COPY --from=build /build/bin/remote-fs-server /usr/local/bin/remote-fs-server
USER 10001:10001
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/remote-fs-server"]
CMD ["serve", "--config", "/etc/remote-fs/config.yml"]
