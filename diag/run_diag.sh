#!/bin/sh
# Diagnostic #3 (diag3 branch only): head-to-head of v8, v9, v10 and the
# current build at practice scale on the scorer, alternating versions each
# round; one line per run, summary last; exits 3 on purpose.
set -u
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
now() { date +%s.%N; }
T0=$(now)
el() { awk -v a="$T0" -v b="$(now)" 'BEGIN{printf "%.1f", b-a}'; }
SUM=/tmp/sum.txt; : > "$SUM"
line() { echo "$*" >> "$SUM"; echo "$*" >&2; }
finish() {
  echo "==================== DIAG3 SUMMARY ====================" >&2
  cat "$SUM" >&2
  echo "=======================================================" >&2
  exit 3
}
guard() { awk -v e="$(el)" 'BEGIN{exit !(e > 150)}' && { line "time guard hit at $(el)s"; finish; }; return 0; }

line "cpu:$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2) nproc=$(nproc)"
/app/sessions >/dev/null 2>&1 || line "dev run failed rc=$?"
/app/generate --seed 777 --events 20000000 --clicks 5000000 --output "$SCRATCH_DIR/g20" >/dev/null 2>&1
line "generated 20M at $(el)s"

run() {
  guard
  v=$1; shift
  rm -rf "$SCRATCH_DIR/o20"; mkdir -p "$SCRATCH_DIR/o20"
  S=$(now)
  env "$@" DATA_DIR="$SCRATCH_DIR/g20" OUTPUT_DIR="$SCRATCH_DIR/o20" /app/sessions-$v > /tmp/r.txt 2>&1
  W=$(awk -v a="$S" -v b="$(now)" 'BEGIN{printf "%.3f", b-a}')
  st=$(grep -E 'parsed|deduped|sessionized|\] done' /tmp/r.txt | sed 's/^\[ *\([0-9.]*\)s\].*/\1/' | tr '\n' '/')
  last=$(grep -E '\] done' /tmp/r.txt | tail -1 | grep -oE 'usr=[0-9.]+ sys=[0-9.]+')
  cpu=$(grep 'cpu seconds' /tmp/r.txt | sed 's/cpu seconds: //; s/dedup_tab/dtab/; s/parse_/p/g; s/links=/l=/; s/dedup=/dd=/; s/group=/g=/; s/sort=/s=/; s/person=/pp=/')
  line "$v | wall=$W | $st | $last | $cpu"
}
for r in 1 2 3; do
  for v in v8 v9 v10 cur; do run $v X=$r; done
done
finish
