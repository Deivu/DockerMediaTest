FROM rust:1.96-slim AS builder
WORKDIR /build
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /build/target/release/mediaserver /app/mediaserver

CMD ["/app/mediaserver"]