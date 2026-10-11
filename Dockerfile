# Build ARIES in the official Go image, then run it in a slim Debian image that
# carries only the runtime tools the runner needs. Harness, sandbox and bridge
# runtimes are siblings: containers through the mounted Docker socket, or pods
# through kubectl. This image therefore bundles neither Node nor benchmark
# images.
# Run the builder on the host's native architecture and cross-compile to the
# target arch. This avoids emulating the Go compiler (which hangs under qemu),
# so building a linux/amd64 image on an arm64 Mac stays fast.
FROM --platform=$BUILDPLATFORM golang:1.26 AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} make build

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates docker.io openssh-client curl \
    && curl -fsSL "https://dl.k8s.io/release/$(curl -fsSL https://dl.k8s.io/release/stable.txt)/bin/linux/$(dpkg --print-architecture)/kubectl" -o /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && apt-get purge -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/bin/aries /app/bin/aries
COPY --from=builder /app/bin/aries-ssh-client /app/bin/aries-ssh-client
COPY --from=builder /app/bin/aries-bridge /app/bin/aries-bridge
# The Docker deployment builds the bridge image from this recipe at run time.
COPY Dockerfile.bridge Dockerfile.bridge
COPY profiles/ profiles/
COPY traces/ traces/
COPY configs/ configs/
COPY docs/ docs/
COPY LICENSE LICENSE
COPY LICENSE-CODE LICENSE-CODE
COPY README.md README.md
ENTRYPOINT ["/app/bin/aries"]
CMD ["profiles/openclaw-tb2-fix-git-deepseek.json"]
