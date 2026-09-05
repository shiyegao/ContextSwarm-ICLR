# Minimal experiment image. NuRouter/AISW is mounted read-only at run time;
# credentials and coordinator configuration never enter this image.
# Bookworm's glibc (2.36) cannot load the host NuRouter ELF (GLIBC_2.39).
# Trixie keeps the official Node 22 image while providing glibc 2.41, so the
# read-only mounted real NuRouter/Pi launcher remains executable as non-root.
FROM node:22-trixie-slim

# Use the operator's currently installed real clients for this freeze.  The
# host-side launch still mounts the real NuRouter binary and node config; these
# npm packages provide the in-image Pi/CodeX executables used by the managed
# launcher.  Both versions remain overrideable at build time.
ARG PI_VERSION=0.84.3
ARG CODEX_VERSION=0.150.1
ARG CONTEXTSWARM_SOURCE_COMMIT=unknown
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/contextswarm \
    CONTEXTSWARM_REPO_ROOT=/opt/contextswarm \
    CONTEXTSWARM_SOURCE_COMMIT=${CONTEXTSWARM_SOURCE_COMMIT}

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 ca-certificates git bash procps \
    && rm -rf /var/lib/apt/lists/* \
    && npm install --global "@earendil-works/pi-coding-agent@${PI_VERSION}" "@openai/codex@${CODEX_VERSION}" \
    && node --version \
    && pi --version

# The launcher normally overrides this identity with the invoking host UID:GID
# so bind-mounted run artifacts remain owned by the operator.  Keep a non-root
# image default as a safe fallback for direct docker invocations.
RUN groupadd --system --gid 65532 contextswarm \
    && useradd --system --uid 65532 --gid 65532 --home-dir /run/contextswarm-mini/home \
        --no-create-home --shell /usr/sbin/nologin contextswarm \
    && install -d -o 65532 -g 65532 -m 0700 /run/contextswarm-mini \
    && install -d -m 0755 \
        /opt/contextswarm-input/aisw \
        /opt/contextswarm-input/aisw-private \
        /opt/contextswarm-input/formal \
        /opt/contextswarm-input/codex-home \
    && touch \
        /opt/contextswarm-input/aisw/pi \
        /opt/contextswarm-input/aisw-private/node.toml

WORKDIR /opt/contextswarm
COPY . /opt/contextswarm

RUN python3 -m compileall -q contextswarm_mini

COPY docker-entrypoint.sh /usr/local/bin/contextswarm-mini-entrypoint
RUN chmod 0755 /usr/local/bin/contextswarm-mini-entrypoint

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/contextswarm-mini-entrypoint"]
CMD ["--config", "configs/cps.toml", "run"]
