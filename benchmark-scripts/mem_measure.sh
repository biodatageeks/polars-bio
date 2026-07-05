#!/bin/bash
# Peak memory on the flagship: private footprint (macOS "peak memory footprint")
# + max RSS ("maximum resident set size"). FastQC is a JVM — the fastqc wrapper
# reports ~7 MB, so its java child is polled separately for max RSS.
# Usage: mem_measure.sh <file.fastq.gz>
set -u
FILE=$1
HERE=$(cd "$(dirname "$0")" && pwd)
LIM="$HERE/lim_nokmer.txt"
[ -s "$LIM" ] || { curl -s "https://raw.githubusercontent.com/s-andrews/FastQC/v0.12.1/Configuration/limits.txt" > "$LIM"; }

# pull "<n> maximum resident set size" and "<n> peak memory footprint" (bytes) -> MB
foot() { awk '/peak memory footprint/{printf "%.0f", $1/1048576}' "$1"; }
rss()  { awk '/maximum resident set size/{printf "%.0f", $1/1048576}' "$1"; }

printf "%-22s %-20s %-12s\n" "tool (config)" "private footprint MB" "max RSS MB"

for t in 1 8; do
  env -u CONDA_PREFIX /usr/bin/time -l python3 "$HERE/pb_mem.py" "$FILE" "$t" >/dev/null 2>m.pb$t
  printf "%-22s %-20s %-12s\n" "polars-bio (${t} core)" "$(foot m.pb$t)" "$(rss m.pb$t)"
done

for t in 1 8; do
  rm -rf rqm; mkdir rqm
  /usr/bin/time -l rastqc -t "$t" --nozip --limits "$LIM" -o rqm "$FILE" >/dev/null 2>m.rq$t
  printf "%-22s %-20s %-12s\n" "RastQC (${t} thread)" "$(foot m.rq$t)" "$(rss m.rq$t)"
done

# FastQC (JVM): /usr/bin/time -l directly on the wrapper. Verified on macOS that
# this folds in the java child's rusage (maxRSS/footprint), matching how RastQC's
# own benchmark measures every tool — no separate JVM polling needed.
rm -rf fqm; mkdir fqm
/usr/bin/time -l fastqc --nogroup --limits "$LIM" -o fqm "$FILE" >/dev/null 2>m.fq
printf "%-22s %-20s %-12s\n" "FastQC (JVM)" "$(foot m.fq)" "$(rss m.fq)"

rm -f m.pb1 m.pb8 m.rq1 m.rq8 m.fq; rm -rf rqm fqm
echo MEM_DONE
