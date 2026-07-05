#!/bin/bash
# Measure wall + total CPU (user+sys) for FastQC / RastQC / polars-bio on one
# indexed BGZF FASTQ, over a list of thread counts. All three run the same 11
# default modules (Kmer Content OFF). Writes a TSV to results_<LABEL>.tsv.
#
# Usage: bench_wallcpu.sh <file.fastq.gz> <LABEL> "<threads csv>"   e.g. "1 2 4 8"
# Env:   PB_REPS (default 3), RQ_RUNS (default 2), FQC_RUNS (default 2)
set -u
FILE=$1; LABEL=$2; THREADS=${3:-"1 2 4 8"}
PB_REPS=${PB_REPS:-3}; RQ_RUNS=${RQ_RUNS:-2}; FQC_RUNS=${FQC_RUNS:-2}
HERE=$(cd "$(dirname "$0")" && pwd)
OUT="results_${LABEL}.tsv"
: > "$OUT"

# median of stdin numbers
median() { sort -n | awk '{a[NR]=$1} END{n=NR; if(n%2){print a[(n+1)/2]}else{printf "%.3f\n",(a[n/2]+a[n/2+1])/2}}'; }
timeval() { grep "^$1" "$2" | awk '{print $2}'; }

# --- stock FastQC limits with Kmer OFF (kmer ignore 1 is the FastQC default) ---
LIM="$HERE/lim_nokmer.txt"
if [ ! -s "$LIM" ]; then
  curl -s "https://raw.githubusercontent.com/s-andrews/FastQC/v0.12.1/Configuration/limits.txt" > "$LIM" 2>/dev/null
  # ensure kmer is ignored (=1); it already is by default
  grep -q '^kmer' "$LIM" || printf 'kmer\tignore\t1\n' >> "$LIM"
fi

echo "# $LABEL  file=$FILE  $(ls -la "$FILE" | awk '{print $5}') bytes  reads=$(echo "$(bgzip -dc "$FILE"|wc -l)/4"|bc)" | tee -a "$OUT"
echo "# cores=$(sysctl -n hw.ncpu)  PB_REPS=$PB_REPS RQ_RUNS=$RQ_RUNS FQC_RUNS=$FQC_RUNS" | tee -a "$OUT"

# ---- FastQC: single-threaded, min wall of FQC_RUNS ----
rm -rf fq_o; mkdir fq_o
fqwall=""
for r in $(seq 1 $FQC_RUNS); do
  /usr/bin/time -p fastqc --nogroup --limits "$LIM" -o fq_o "$FILE" >/dev/null 2>t.$$;
  w=$(timeval real t.$$); fqwall="$fqwall$w\n"
done
FQ_WALL=$(printf "$fqwall" | grep . | median)
printf "tool\tthreads\twall_s\tcpu_s\n" | tee -a "$OUT"
printf "FastQC\t1\t%s\t%s\n" "$FQ_WALL" "$FQ_WALL" | tee -a "$OUT"   # single-threaded: cpu≈wall

# ---- RastQC: per thread, best-of-RQ_RUNS wall; report that run's user+sys ----
for t in $THREADS; do
  best_w=""; best_c=""
  for r in $(seq 1 $RQ_RUNS); do
    rm -rf rq_o; mkdir rq_o
    /usr/bin/time -p rastqc -t "$t" --nozip --limits "$LIM" -o rq_o "$FILE" >/dev/null 2>t.$$
    w=$(timeval real t.$$); u=$(timeval user t.$$); s=$(timeval sys t.$$)
    c=$(echo "$u + $s" | bc)
    if [ -z "$best_w" ] || (( $(echo "$w < $best_w" | bc -l) )); then best_w=$w; best_c=$c; fi
  done
  printf "RastQC\t%s\t%s\t%s\n" "$t" "$best_w" "$best_c" | tee -a "$OUT"
done

# ---- polars-bio: per thread, fresh process, median wall + median cpu ----
for t in $THREADS; do
  tmp=$(env -u CONDA_PREFIX POLARS_MAX_THREADS=1 python3 "$HERE/pb_wallcpu.py" "$FILE" "$t" "$PB_REPS" 2>/dev/null)
  w=$(echo "$tmp" | awk '{print $3}' | median)
  c=$(echo "$tmp" | awk '{print $4}' | median)
  printf "polars-bio\t%s\t%s\t%s\n" "$t" "$w" "$c" | tee -a "$OUT"
done

rm -f t.$$; rm -rf fq_o rq_o
echo "BENCH_DONE $LABEL"
