cd /private/tmp/claude-501/-Users-mwiewior-research-git-polars-bio/063ee24e-8412-45ca-b8d4-f77cb47b5147/scratchpad
# self-contained: FastQC limits with kmer ENABLED (ignore 0)
curl -s "https://raw.githubusercontent.com/s-andrews/FastQC/v0.12.1/Configuration/limits.txt" > lim_kmer.txt
sed -i '' 's/^kmer[[:space:]]*ignore[[:space:]]*1/kmer	ignore	0/' lim_kmer.txt
grep -q '^kmer	ignore	0' lim_kmer.txt || { echo "LIMITS SETUP FAILED"; exit 1; }
rm -rf sw; mkdir sw
prefetch ERR5897746 -O sw >/dev/null 2>&1 || { echo PREFETCHFAIL; exit 1; }
fasterq-dump sw/ERR5897746 -O sw --split-files -f -e 4 >/dev/null 2>&1 || { echo DUMPFAIL; exit 1; }
bgzip -i -@4 -c sw/ERR5897746_1.fastq > f.fastq.gz && bgzip -r f.fastq.gz
rm -rf sw
rm -rf fqc_gold; mkdir fqc_gold
fastqc --nogroup --extract --limits lim_kmer.txt -o fqc_gold f.fastq.gz 2>&1 | tail -1
ls fqc_gold/*_fastqc/fastqc_data.txt >/dev/null 2>&1 || { echo "FASTQC PRODUCED NO OUTPUT"; exit 1; }
for t in 1 4; do rm -rf rq$t; mkdir rq$t; rastqc -t $t -o rq$t f.fastq.gz >/dev/null 2>&1; (cd rq$t && unzip -oq ./*_fastqc.zip); done
echo "=== reads ==="; echo "$(( $(zcat f.fastq.gz 2>/dev/null | wc -l) / 4 ))" 2>/dev/null || true
echo "=== DRIFT TABLE ==="
env -u CONDA_PREFIX python3 parse_drift.py
rm -f f.fastq.gz f.fastq.gz.gzi
echo DRIFT_DONE
