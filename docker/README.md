# Runner in a container

ARIES normally runs as a host process: `make build`, then `./bin/aries <profile>`.
It starts every harness, task sandbox and the run's tool bridge as containers on
one Docker Engine. See the [quick start](../docs/quick-start.md).

This directory holds a Compose file that runs the runner itself in a container,
for hosts that cannot build or run `bin/aries` natively, such as macOS. For a
Kubernetes cluster, see [`k8s/`](../k8s/README.md) instead.

Run every command below from the repository root.

```sh
docker compose -f docker/docker-compose.yml run --rm aries
```

`PROFILE` selects the profile and defaults to
`profiles/openclaw-tb2-fix-git-deepseek.json`:

```sh
PROFILE=profiles/hermes-tb2-fix-git-deepseek.json \
  docker compose -f docker/docker-compose.yml run --rm aries
```

## How it works

The container mounts the host's Docker socket, so the runtimes it starts are
siblings on the host daemon, not nested containers. It uses host networking
because the runner reaches the harness gateway and the bridge's control port on
ports published to host loopback.

`Dockerfile` cross-compiles on the host's native architecture, so building the
image on Apple Silicon does not emulate the Go compiler. The image carries
`bin/aries`, `bin/aries-ssh-client`, `bin/aries-bridge` and `Dockerfile.bridge`,
which the runner uses to build the bridge image on the host daemon.

Mounting the Docker socket gives the runner container control of the host
daemon. That is the same authority a host-run `bin/aries` has.

## Model key

The runner reads the DeepSeek key the same way as a host run: from
`DEEPSEEK_API_KEY` in the environment, or from `DEEPSEEK_API.key` in the working
directory. Compose mounts the repository-root `DEEPSEEK_API.key` read-only, so
the key never appears in the container's environment or `docker inspect` output:

```sh
umask 077
printf '%s' "$DEEPSEEK_API_KEY" > DEEPSEEK_API.key
```

The file is gitignored and excluded from the image by `.dockerignore`.

## Results and cleanup

Run artifacts land in `runs/` on the host, as with a host run. The runner
removes the containers and network it created when the run ends. To check for
leftovers after an interrupted run:

```sh
docker ps -a --filter label=aries.managed=true
docker network ls --filter label=aries.managed=true
```
