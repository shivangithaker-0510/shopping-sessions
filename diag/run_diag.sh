#!/bin/sh
# Diagnostic run (diag branch only): profiles the scorer machine and the
# pipeline at practice scale, then exits non-zero on purpose so the dev-set
# check fails and returns these logs. Never submit this as a real entry.
set -u
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
now() { date +%s.%N; }
T0=$(now)
el() { awk -v a="$T0" -v b="$(now)" 'BEGIN{printf "%.3f", b-a}'; }
SUM=/tmp/diag_summary.txt; : > "$SUM"
say() { echo "### [$(el)s] $*" >&2; echo "[$(el)s] $*" >> "$SUM"; }
# Print the compact summary at the end as well, so it survives a log that is
# cut to its last lines.
summary() {
  echo "==================== DIAG SUMMARY ====================" >&2
  cat "$SUM" >&2
  echo "======================================================" >&2
}
# Stay well inside the 3-minute dev-set limit: stop profiling after 140 s.
guard() { awk -v e="$(el)" 'BEGIN{exit !(e > 140)}' && { say "time guard hit; stopping"; summary; exit 3; }; return 0; }

say "machine"
uname -a >&2
nproc >&2
grep -m1 'model name' /proc/cpuinfo >&2
{ echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2) | nproc=$(nproc) | $(grep -m1 'cache size' /proc/cpuinfo | cut -d: -f2)"
  echo "flags: $(grep -m1 '^flags' /proc/cpuinfo | tr ' ' '
' | grep -E '^(avx2|avx512f|bmi2)$' | tr '
' ' ')"
  echo "cgroup: $(cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max 2>/dev/null | tr '
' ' ') thp: $(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null)"
  echo "mounts: $(grep -E ' (/data|/scratch|/output) ' /proc/mounts | awk '{print $2"="$3}' | tr '
' ' ')"
  echo "mem: $(grep -E 'MemTotal|MemAvailable' /proc/meminfo | tr -s ' ' | tr '
' ' ')"; } >> "$SUM"
grep -m1 'cpu MHz' /proc/cpuinfo >&2
grep -m1 '^flags' /proc/cpuinfo | tr ' ' '\n' | grep -E '^(avx2|avx512f|avx512bw|bmi2|sha_ni|vaes)$' | tr '\n' ' ' >&2; echo >&2
grep -E 'cache size|siblings|cpu cores' /proc/cpuinfo | sort -u >&2
cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null | tr '\n' ' ' >&2; echo >&2
cat /sys/kernel/mm/transparent_hugepage/enabled /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null >&2
grep -E 'MemTotal|MemFree|MemAvailable|^Cached' /proc/meminfo >&2
grep -E ' (/data|/output|/scratch|/tmp|/) ' /proc/mounts >&2
df -h "$SCRATCH_DIR" "$DATA_DIR" 2>/dev/null >&2

say "real pipeline on dev data -> /output"
S=$(now); /app/sessions; RC=$?; E=$(now)
say "dev pipeline exit=$RC wall=$(awk -v a="$S" -v b="$E" 'BEGIN{printf "%.3f", b-a}')"

say "read /data (second touch, page cache)"
S=$(now); cat "$DATA_DIR"/events/*.csv "$DATA_DIR"/clicks/*.csv > /dev/null; E=$(now)
say "read dev csv wall=$(awk -v a="$S" -v b="$E" 'BEGIN{printf "%.3f", b-a}') bytes=$(cat "$DATA_DIR"/events/*.csv "$DATA_DIR"/clicks/*.csv | wc -c)"

say "page-fault cost"
FAULTBENCH=1024 FB_THREADS=8 /app/sessions 2>&1 | tail -4 | tee -a "$SUM" >&2

guard
say "generate 20M events / 5M clicks into scratch"
S=$(now); /app/generate --seed 12345 --events 20000000 --clicks 5000000 --output "$SCRATCH_DIR/g20" >/dev/null 2>&1; E=$(now)
say "generate wall=$(awk -v a="$S" -v b="$E" 'BEGIN{printf "%.3f", b-a}') size=$(du -sh "$SCRATCH_DIR/g20" | cut -f1)"

run20() {
  guard
  rm -rf "$SCRATCH_DIR/o20"; mkdir -p "$SCRATCH_DIR/o20"
  S=$(now)
  env "$@" DATA_DIR="$SCRATCH_DIR/g20" OUTPUT_DIR="$SCRATCH_DIR/o20" /app/sessions 2>&1 | grep -vE '^$' > /tmp/run20.txt
  cat /tmp/run20.txt >&2
  grep -E 'parsed|deduped|sessionized|\] done|cpu seconds' /tmp/run20.txt | sed 's/VmHWM:[^ ]* //' >> "$SUM"
  E=$(now)
  say "run20 [$*] process wall=$(awk -v a="$S" -v b="$E" 'BEGIN{printf "%.3f", b-a}')"
}
say "pipeline on 20M (run 1, files just written)"; run20 X=1
say "pipeline on 20M (run 2)"; run20 X=2
say "pipeline on 20M (run 3)"; run20 X=3
say "pipeline on 20M, 16 threads"; run20 SS_THREADS=16
say "pipeline on 20M, 12 threads"; run20 SS_THREADS=12
say "pipeline on 20M, scan-only parse (SS_EXP=1)"; run20 SS_EXP=1
say "pipeline on 20M, parse without stores (SS_EXP=2)"; run20 SS_EXP=2
say "pipeline on 20M, read-only dedup (SS_EXP=3)"; run20 SS_EXP=3
say "pipeline on 20M (run 4)"; run20 X=4

say "done; exiting 3 on purpose (diagnostic build)"
summary
exit 3
