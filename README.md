# Shopping Sessions – speed contest entry

Pipeline: clean CSV event logs -> link user IDs (union-find over the ID-link file) ->
build sessions -> last-touch ad attribution -> 7-day assisting campaigns ->
write `/output/user_daily/` and `/output/campaign_daily/` as Parquet.

Scored on container wall-clock time (8 vCPU, 24 GB). Correctness must be exact.

## Layout
- `Dockerfile` – built and run by the scorer (linux/amd64)
- `src/` – pipeline code
- `rules.md` – the official spec (source of truth)

## Local check
1. Generate the 1M dev set with the generator.
2. Unzip dev-answer.zip.
3. `make check-dev`
