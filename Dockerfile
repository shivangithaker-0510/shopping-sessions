# DIAGNOSTIC BUILD (diag branch): pipeline + official generator + profiling
# script. Exits non-zero on purpose. Not a real entry.
FROM rust:1.88-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends make && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release --locked && strip target/release/sessions
COPY diag/generator /gen
RUN cd /gen && cargo build --release --locked && strip target/release/generate

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/sessions /app/sessions
COPY --from=build /gen/target/release/generate /app/generate
COPY diag/run_diag.sh /app/run.sh
RUN chmod +x /app/run.sh /app/sessions /app/generate
WORKDIR /app
CMD ["/app/run.sh"]
