FROM rust:1.90 as builder

WORKDIR /usr/src/broker

# Copy Cargo files first for caching
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build the release binary
RUN cargo build --release

# Minimal runtime image
FROM debian:bookworm-slim

WORKDIR /usr/src/broker

# Copy binary from builder
COPY --from=builder /usr/src/broker/target/release/broker .

# EXPOSE
EXPOSE 4000

CMD ["./broker"]
