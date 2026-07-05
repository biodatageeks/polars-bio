cd /private/tmp/claude-501/-Users-mwiewior-research-git-polars-bio/063ee24e-8412-45ca-b8d4-f77cb47b5147/scratchpad
curl -s "https://raw.githubusercontent.com/s-andrews/FastQC/v0.12.1/Configuration/limits.txt" > lim_kmer.txt
sed -i.bak 's/^kmer[[:space:]]*ignore[[:space:]]*1/kmer	ignore	0/' lim_kmer.txt && rm -f lim_kmer.txt.bak
rm -rf sw; mkdir sw
prefetch ERR5897746 -O sw >/dev/null 2>&1 || { echo PREFETCHFAIL; exit 1; }
fasterq-dump sw/ERR5897746 -O sw --split-files -f -e 4 >/dev/null 2>&1 || { echo DUMPFAIL; exit 1; }
bgzip -i -@4 -c sw/ERR5897746_1.fastq > f.fastq.gz && bgzip -r f.fastq.gz
rm -rf sw
rm -rf fqc_gold rq1 rq4; mkdir fqc_gold rq1 rq4
fastqc --nogroup --extract --limits lim_kmer.txt -o fqc_gold f.fastq.gz >/dev/null 2>&1
# RastQC also with kmer-enabled limits so both compute the same modules
rastqc -t 1 --limits lim_kmer.txt -o rq1 f.fastq.gz >/dev/null 2>&1; (cd rq1 && unzip -oq ./*_fastqc.zip)
rastqc -t 4 --limits lim_kmer.txt -o rq4 f.fastq.gz >/dev/null 2>&1; (cd rq4 && unzip -oq ./*_fastqc.zip)
env -u CONDA_PREFIX python3 - <<'PY'
import polars_bio as pb
for p in (1,4):
    pb.set_option("datafusion.execution.target_partitions", str(p))
    pb.fastqc("f.fastq.gz").tidy.collect().write_csv(f"pb{p}.csv")
print("pb dumps written")
PY
ls -la fqc_gold/*_fastqc/fastqc_data.txt rq1/*_fastqc/fastqc_data.txt rq4/*_fastqc/fastqc_data.txt pb1.csv pb4.csv
echo RUN_ALL_DONE
