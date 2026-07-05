cd /private/tmp/claude-501/-Users-mwiewior-research-git-polars-bio/063ee24e-8412-45ca-b8d4-f77cb47b5147/scratchpad
rm -rf sw; mkdir sw
prefetch ERR5897746 -O sw >/dev/null 2>&1 && fasterq-dump sw/ERR5897746 -O sw --split-files -f -e 4 >/dev/null 2>&1
[ -s sw/ERR5897746_1.fastq ] || { echo FETCHFAIL; exit 1; }
bgzip -i -@4 -c sw/ERR5897746_1.fastq > f.fastq.gz && bgzip -r f.fastq.gz; rm -rf sw
echo "polars-bio, 11 modules (parallel path), POLARS_MAX_THREADS=1, 3 reps/count:"
for t in 1 2 4 6 8 10; do env -u CONDA_PREFIX POLARS_MAX_THREADS=1 python3 pb_reps.py f.fastq.gz $t 2>/dev/null; done
rm -f f.fastq.gz f.fastq.gz.gzi
echo REPS_DONE
