# DIAGNOSTIC #4 (diag4 branch): the previous build (4f6b142) and the new
# scanner build (with the AVX-512 mask option compiled in) in one image, run
# alternately on a generated 20M dataset, with output checksums compared.
# Exits non-zero on purpose. Not a real entry.
# Rust 1.89+ is needed for the AVX-512 intrinsics.
FROM rust:1.89-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends make && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY diag/versions /versions
COPY src /versions/cur/src
RUN mkdir -p /out \
    && rm -rf src && cp -r /versions/prev/src src && touch src/main.rs \
    && cargo build --release --locked && strip target/release/sessions && cp target/release/sessions /out/sessions-prev \
    && rm -rf src && cp -r /versions/cur/src src && touch src/main.rs \
    && cargo build --release --locked --features avx512 && strip target/release/sessions && cp target/release/sessions /out/sessions-cur \
    && cp /out/sessions-cur /out/sessions
COPY diag/generator /gen
RUN cd /gen && cargo build --release --locked && strip target/release/generate

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /out/ /app/
COPY --from=build /gen/target/release/generate /app/generate
COPY diag/run_diag.sh /app/run.sh
RUN chmod +x /app/run.sh /app/sessions* /app/generate
WORKDIR /app
CMD ["/app/run.sh"]
