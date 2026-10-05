# syntax=docker/dockerfile:1
FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev file
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release --locked \
    && file target/release/kubernetes-reflector-rs | grep -q "static-pie linked\|statically linked"

FROM scratch
COPY --from=builder /src/target/release/kubernetes-reflector-rs /kubernetes-reflector-rs
USER 65534:65534
ENTRYPOINT ["/kubernetes-reflector-rs"]
