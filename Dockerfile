# Multi-stage build for img2dataset-rs
FROM rust:1.88-slim AS builder

# Set working directory
WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies only (this layer will be cached)
RUN cargo build --release

# Remove dummy main.rs and copy actual source code
RUN rm src/main.rs
COPY src/ ./src/

# Build the actual application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Create non-root user
RUN useradd -m -u 1000 img2dataset

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/target/release/img2dataset-rs /usr/local/bin/

# Create directories for input/output
RUN mkdir -p /data/input /data/output && \
    chown -R img2dataset:img2dataset /data

# Switch to non-root user
USER img2dataset

# Set default volumes
VOLUME ["/data/input", "/data/output"]

# Set default working directory
WORKDIR /data

# Default command
ENTRYPOINT ["img2dataset-rs"]
CMD ["--help"]
