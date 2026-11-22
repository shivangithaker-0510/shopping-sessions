# DIAGNOSTIC #3 (diag3 branch): v8, v9, v10 and the current code built into one
# image (shared dependency build), run alternately on a generated 20M dataset.
# Exits non-zero on purpose. Not a real entry.
FROM rust:1.88-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends make && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY diag/versions /versions
COPY src /versions/cur/src
RUN mkdir -p /out && for v in v8 v9 v10 cur; do \
      rm -rf src && cp -r /versions/$v/src src && touch src/main.rs \
      && cargo build --release --locked && strip target/release/sessions \
      && cp target/release/sessions /out/sessions-$v || exit 1; \
    done && cp /out/sessions-cur /out/sessions
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
