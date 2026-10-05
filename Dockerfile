# syntax=docker/dockerfile:1
FROM rust:1.99-alpine AS builder
RUN apk add --no-cache musl-dev file
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release --locked \
    && file target/release/k8s-reflecto-rs | grep -q "static-pie linked\|statically linked"

FROM scratch
COPY --from=builder /src/target/release/k8s-reflecto-rs /k8s-reflecto-rs
USER 65534:65534
ENTRYPOINT ["/k8s-reflecto-rs"]
