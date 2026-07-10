FROM rust:1.96-slim AS builder

WORKDIR /home/server
COPY . .
RUN cargo build --release

FROM debian:trixie-slim

WORKDIR /home/server
COPY --from=builder /home/server/target/release/mediaserver /home/server/mediaserver
COPY --from=builder /home/server/data /home/server/data

ENTRYPOINT ["./mediaserver"]