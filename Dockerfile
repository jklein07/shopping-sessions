# Shopping Sessions: a native Rust pipeline, no crates beyond std.
# Base image pinned by digest; the build downloads nothing else.
FROM rust:1.91-slim-bookworm@sha256:8514999d4786ef12efe89239e86b3d0a021b94b9d35108c8efe6c79ca7dc1a65 AS build
ARG TARGETARCH
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN case "${TARGETARCH:-amd64}" in \
      amd64) T=x86_64-unknown-linux-gnu; F="-C target-cpu=x86-64-v3" ;; \
      arm64) T=aarch64-unknown-linux-gnu; F="" ;; \
      *) echo "unsupported arch ${TARGETARCH}"; exit 1 ;; \
    esac \
 && RUSTFLAGS="-C target-feature=+crt-static $F" cargo build --release --locked --offline --target "$T" \
 && cp "target/$T/release/ss" /ss

# A static binary in an empty image: nothing to start but the pipeline.
FROM scratch
COPY --from=build /ss /ss
USER 1000:1000
CMD ["/ss"]
