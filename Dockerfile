# Shopping Sessions: native Rust pipeline.
# Build stage compiles a release binary; the runtime image only holds that binary.
FROM rust:1.88-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
# Pre-fetch and pre-build dependencies so source edits rebuild fast.
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release --locked && strip target/release/sessions

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/sessions /app/sessions
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/sessions
WORKDIR /app
CMD ["/app/run.sh"]
