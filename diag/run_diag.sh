#!/bin/sh
# Diagnostic run #2 (diag2 branch only). Profiles the pipeline at practice
# scale on the scorer with one compact line per variant, then exits 3 on
# purpose so the dev-set check fails and returns the log. Not a scoring entry.
set -u
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
now() { date +%s.%N; }
T0=$(now)
el() { awk -v a="$T0" -v b="$(now)" 'BEGIN{printf "%.1f", b-a}'; }
SUM=/tmp/sum.txt; : > "$SUM"
line() { echo "$*" >> "$SUM"; echo "$*" >&2; }
finish() {
  echo "==================== DIAG2 SUMMARY ====================" >&2
  cat "$SUM" >&2
  echo "=======================================================" >&2
  exit 3
}
guard() { awk -v e="$(el)" 'BEGIN{exit !(e > 140)}' && { line "time guard hit at $(el)s"; finish; }; return 0; }

line "cpu:$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2) nproc=$(nproc) l3:$(grep -m1 'cache size' /proc/cpuinfo | cut -d: -f2)"
line "flags: $(grep -m1 '^flags' /proc/cpuinfo | tr ' ' '\n' | grep -E '^(avx2|avx512f|avx512bw|bmi2)$' | tr '\n' ' ')"
line "thp: $(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null) defrag: $(cat /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null)"
line "mounts: $(grep -E ' (/data|/scratch) ' /proc/mounts | awk '{print $2"="$3}' | tr '\n' ' ')"

/app/sessions >/dev/null 2>&1 || line "dev run failed rc=$?"
/app/generate --seed 4242 --events 20000000 --clicks 5000000 --output "$SCRATCH_DIR/g20" >/dev/null 2>&1
line "generated 20M at $(el)s"

# One run -> one line: label, stage end times, cpu, faults, process wall.
run() {
  guard
  label=$1; shift
  rm -rf "$SCRATCH_DIR/o20"; mkdir -p "$SCRATCH_DIR/o20"
  S=$(now)
  env "$@" DATA_DIR="$SCRATCH_DIR/g20" OUTPUT_DIR="$SCRATCH_DIR/o20" /app/sessions > /tmp/r.txt 2>&1
  W=$(awk -v a="$S" -v b="$(now)" 'BEGIN{printf "%.3f", b-a}')
  st=$(grep -E 'parsed|deduped|sessionized|\] done' /tmp/r.txt | sed 's/^\[ *\([0-9.]*\)s\].*/\1/' | tr '\n' '/')
  last=$(grep -E '\] (done|links built)' /tmp/r.txt | tail -1 | grep -oE 'minflt=[0-9]+ usr=[0-9.]+ sys=[0-9.]+ thpkB=[0-9]*')
  cpu=$(grep 'cpu seconds' /tmp/r.txt | sed 's/cpu seconds: //; s/dedup_tab/dtab/; s/parse_/p/g')
  line "$label | wall=$W | stages=$st | $last | $cpu"
}
run base X=1
run base X=2
run thp MALLOC_CONF=thp:always,metadata_thp:always
run thp MALLOC_CONF=thp:always,metadata_thp:always
run populate SS_POPULATE=1
run epart64 SS_EPART=64
run scan-only SS_EXP=1
run no-store SS_EXP=2
run dedup-readonly SS_EXP=3
run base X=3
run thp+pop MALLOC_CONF=thp:always,metadata_thp:always SS_POPULATE=1
finish
