# syntax=docker/dockerfile:1.7
#
# OpenSearch with the Infino engine plugin, built from this checkout.
#
#   docker compose build                                   # see docker-compose.yml
#   docker build -f docker/Dockerfile --build-arg OPENSEARCH_VERSION=3.6.0 .   # any supported release
#
# The plugin zip installs only into the release it was built for, so one
# OPENSEARCH_VERSION drives the base image, the Gradle build and the install.
# The plugin supports 2.19.6, 3.0.0, 3.3.2, 3.6.0 and 3.9.0; env.example says
# which of them this image has been tested on. Each wants its own Gradle
# (README "Build and test"): set GRADLE_IMAGE to match.
#   2.19.x -> gradle:8.12.1-jdk21    3.0.x -> gradle:8.14-jdk21    3.3+ -> gradle:9-jdk21

ARG OPENSEARCH_VERSION=3.9.0
ARG GRADLE_IMAGE=gradle:9-jdk21
# The engine's toolchain (the infino crate's rust-version), pinned so the
# native build is reproducible.
ARG RUST_TOOLCHAIN=1.95.0

# --- Native bridge (Rust cdylib) -------------------------------------------
# Built on the OpenSearch image itself so the .so links against exactly the
# glibc it will run on.
FROM opensearchproject/opensearch:${OPENSEARCH_VERSION} AS native
ARG RUST_TOOLCHAIN
USER root
RUN yum install -y gcc gcc-c++ make tar gzip && yum clean all
ENV RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo PATH=/opt/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain ${RUST_TOOLCHAIN} --no-modify-path
WORKDIR /src/infino-capi
COPY infino-capi/ ./
RUN --mount=type=cache,target=/opt/cargo/registry \
    --mount=type=cache,target=/src/infino-capi/target \
    cargo build --release --locked \
 && mkdir -p /out && cp target/release/libinfino_capi.so /out/

# --- Plugin zip (Gradle) ---------------------------------------------------
FROM ${GRADLE_IMAGE} AS plugin
ARG OPENSEARCH_VERSION
WORKDIR /src/plugin
COPY plugin/ ./
RUN --mount=type=cache,target=/home/gradle/.gradle \
    gradle --no-daemon -q -Dopensearch.version=${OPENSEARCH_VERSION} bundlePlugin \
 && mkdir -p /out && cp build/distributions/*.zip /out/infino-engine.zip

# --- Runtime ---------------------------------------------------------------
FROM opensearchproject/opensearch:${OPENSEARCH_VERSION}
ARG OPENSEARCH_VERSION
# The bundle's neural-search plugin takes the node's query-phase searcher, which
# forces every index into proxy mode (README "Neural search"). Removed by
# default so default mode is what gets tested; set to false to keep it.
ARG REMOVE_NEURAL_SEARCH=true

COPY --from=native --chown=1000:1000 /out/libinfino_capi.so /usr/share/opensearch/infino/lib/libinfino_capi.so
COPY --from=plugin --chown=1000:1000 /out/infino-engine.zip /tmp/infino-engine.zip
COPY --chmod=755 docker/infino-entrypoint.sh /usr/share/opensearch/infino-entrypoint.sh

RUN /usr/share/opensearch/bin/opensearch-plugin install -b file:///tmp/infino-engine.zip \
 && rm /tmp/infino-engine.zip \
 && if [ "$REMOVE_NEURAL_SEARCH" = "true" ] && [ -d /usr/share/opensearch/plugins/opensearch-neural-search ]; then \
      /usr/share/opensearch/bin/opensearch-plugin remove opensearch-neural-search; \
    fi \
 && mkdir -p /usr/share/opensearch/infino/data

LABEL org.opencontainers.image.title="opensearch-infino" \
      org.opencontainers.image.description="OpenSearch ${OPENSEARCH_VERSION} with the Infino engine plugin"

ENTRYPOINT ["/usr/share/opensearch/infino-entrypoint.sh"]
CMD ["opensearch"]
