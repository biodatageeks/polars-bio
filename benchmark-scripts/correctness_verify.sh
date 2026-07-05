#!/bin/bash
# Re-verify polars-bio FastQC correctness against the FastQC 0.12.1 golden.
# 12-module drift + thread self-consistency (pb p1 vs p4, RastQC t1 vs t4).
# Reuses an already-indexed ERR5897746 BGZF (no re-fetch).
# Usage: correctness_verify.sh <file.fastq.gz>
set -u
FILE=${1:-f.fastq.gz}
HERE=$(cd "$(dirname "$0")" && pwd)

# FastQC limits with Kmer ON (ignore=0) so all tools compute the same 12 modules
curl -s "https://raw.githubusercontent.com/s-andrews/FastQC/v0.12.1/Configuration/limits.txt" > lim_kmer.txt
sed -i.bak 's/^kmer[[:space:]]*ignore[[:space:]]*1/kmer	ignore	0/' lim_kmer.txt && rm -f lim_kmer.txt.bak

rm -rf fqc_gold rq1 rq4; mkdir fqc_gold rq1 rq4
echo "FastQC golden..."
fastqc --nogroup --extract --limits lim_kmer.txt -o fqc_gold "$FILE" >/dev/null 2>&1
echo "RastQC t1/t4..."
rastqc -t 1 --limits lim_kmer.txt -o rq1 "$FILE" >/dev/null 2>&1; (cd rq1 && unzip -oq ./*_fastqc.zip)
rastqc -t 4 --limits lim_kmer.txt -o rq4 "$FILE" >/dev/null 2>&1; (cd rq4 && unzip -oq ./*_fastqc.zip)
echo "polars-bio p1/p4 dumps (full 12-module default incl. kmer)..."
env -u CONDA_PREFIX python3 - "$FILE" <<'PY'
import sys, polars_bio as pb
for p in (1, 4):
    pb.set_option("datafusion.execution.target_partitions", str(p))
    pb.fastqc(sys.argv[1]).tidy.collect().write_csv(f"pb{p}.csv")
print("pb dumps written")
PY
echo "=== DRIFT TABLE (max |tool - FastQC golden|; 0 = bit-exact) ==="
env -u CONDA_PREFIX python3 "$HERE/parse_all.py"
echo CORRECTNESS_DONE
