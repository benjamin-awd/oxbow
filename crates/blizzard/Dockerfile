# Stage 1: Generate a recipe file for dependencies
FROM rust:1.92.0 AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Build dependencies (cached unless Cargo.toml/Cargo.lock change)
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --recipe-path recipe.json

# Stage 3: Build the actual application
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin file-loader-cloudrun && \
    cp /app/target/release/file-loader-cloudrun /file-loader-cloudrun

# Runtime stage - use distroless for minimal attack surface
FROM gcr.io/distroless/cc-debian13

# Copy required shared libraries not in distroless
COPY --from=builder /usr/lib/x86_64-linux-gnu/libbz2.so.1.0 /usr/lib/x86_64-linux-gnu/libbz2.so.1.0

# Copy the built binary
COPY --from=builder /file-loader-cloudrun /file-loader-cloudrun

# Cloud Run expects the service to listen on PORT
ENV PORT=8080

# Run the binary
CMD ["/file-loader-cloudrun"]
