#!/usr/bin/env bash
# Matched multi-core short-read benchmark (11 default FastQC modules, kmer OFF).
# Uses SRA-toolkit prefetch+fasterq-dump (fast, reliable) instead of ENA wget.
cd /private/tmp/claude-501/-Users-mwiewior-research-git-polars-bio/063ee24e-8412-45ca-b8d4-f77cb47b5147/scratchpad
LIM=/opt/homebrew/Cellar/fastqc/0.12.1/libexec/Configuration/limits.txt
RES=matched_results.tsv
WORK=sra_work
printf "file\treads\tfastqc\trastqc_1\trastqc_2\trastqc_4\trastqc_8\tpb_1\tpb_2\tpb_4\tpb_8\n" > $RES
wall(){ { /usr/bin/time -p "$@" >/dev/null 2>/tmp/w.t; } 2>/dev/null; grep '^real' /tmp/w.t|awk '{print $2}'; }
rq(){ rm -rf o; mkdir o; wall rastqc -t "$1" --limits "$LIM" -o o "$2"; }
pb(){ env -u CONDA_PREFIX python pbt2.py "$2" "$1" 2>/dev/null; }

process(){
  acc=$1; mate=$2; label=$3
  echo ">>> $acc R$mate ($label) $(date +%T)"
  rm -rf "$WORK"; mkdir -p "$WORK"
  prefetch "$acc" -O "$WORK" >/dev/null 2>&1 || { echo "$acc R$mate PREFETCHFAIL" >> $RES; return; }
  fasterq-dump "$WORK/$acc" -O "$WORK" --split-files -f -e 4 >/dev/null 2>&1 || { echo "$acc R$mate DUMPFAIL" >> $RES; return; }
  fq="$WORK/${acc}_${mate}.fastq"
  [ -f "$fq" ] || fq="$WORK/${acc}.fastq"   # single-end fallback
  [ -f "$fq" ] || { echo "$acc R$mate NOFASTQ" >> $RES; return; }
  reads=$(( $(wc -l < "$fq") / 4 ))
  # indexed BGZF (required for polars-bio partition splitting)
  bgzip -i -@4 -c "$fq" > f.fastq.gz && bgzip -r f.fastq.gz
  rm -rf "$WORK"
  rm -rf o; mkdir o
  ft=$(wall fastqc --nogroup -o o f.fastq.gz)
  # confirm kmer disabled in RastQC (log once, first dataset)
  if [ ! -f kmer_check.txt ]; then
    rm -rf o; mkdir o; rastqc -t 1 --limits "$LIM" -o o f.fastq.gz >/dev/null 2>&1
    (cd o && unzip -o -q ./*_fastqc.zip 2>/dev/null)
    DT=$(find o -name fastqc_data.txt | head -1)
    echo "RastQC Kmer lines with --limits: $(grep -c 'Kmer' "$DT" 2>/dev/null) (0 == kmer OFF)" > kmer_check.txt
    echo "RastQC modules:" >> kmer_check.txt; grep '^>>' "$DT" 2>/dev/null | grep -v '>>END' >> kmer_check.txt
  fi
  r1=$(rq 1 f.fastq.gz); r2=$(rq 2 f.fastq.gz); r4=$(rq 4 f.fastq.gz); r8=$(rq 8 f.fastq.gz)
  p1=$(pb 1 "$PWD/f.fastq.gz"); p2=$(pb 2 "$PWD/f.fastq.gz"); p4=$(pb 4 "$PWD/f.fastq.gz"); p8=$(pb 8 "$PWD/f.fastq.gz")
  printf "%s R%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$acc" "$mate" "$reads" "$ft" "$r1" "$r2" "$r4" "$r8" "$p1" "$p2" "$p4" "$p8" | tee -a $RES
  rm -f f.fastq.gz f.fastq.gz.gzi
}

rm -f kmer_check.txt
process DRR609229 1 720K
process DRR609229 2 720K
process ERR5897746 1 4.3M
process ERR5897746 2 4.3M
process DRR013000 1 24.8M
printf "DONE\n" >> $RES
echo "ALL DONE $(date +%T)"
